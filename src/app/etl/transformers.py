from collections import namedtuple
import datetime
import functools
import logging

from app import models, constants
from app.util.timestamps import utc_now

LOGGER = logging.getLogger(__name__)
NODE_PATH_CACHE_SIZE = 16  # should be a power of 2


def transform_submissions(session, submissions, processed_on:datetime.datetime=None, to_dict=False):
    """
    Transforms Submissions into ResponseEvents

    :param session: SQLAlchemy session
    :param submissions: submissions generator
    :param processed_on: optional timestamp to apply to 'processed_on' column of all ResponseEvents
    :return: generator of ResponseEvents
    """
    processed_on = processed_on or utc_now()
    get_node_path_map = get_node_path_map_cache(session)
    num_submissions = 0
    for submission in submissions:
        yield from _transform_submission(get_node_path_map, submission, processed_on, to_dict)
        num_submissions += 1
    LOGGER.info('Transformed %d JSON submissions', num_submissions)


def map_nested(node:dict, gen_items, gen_children):
    """
    Recursively transforms a nested python dictionary into a generator of items

    :param obj: current object node to transform
    :param path: path components
    :param gen_items: generator to extract items from node
        called with (node, path)
    :param gen_children: generator to extract children from node
        called with (node, path)
    """
    return _recursive_map_nested(node, [], gen_items, gen_children)


def _recursive_map_nested(node:dict, path: list, gen_items, gen_children):
    if not node:
        return

    yield from gen_items(node, path)

    for child_path, child_node in gen_children(node, path):
        yield from _recursive_map_nested(child_node, child_path, gen_items, gen_children)


def _make_path_str(path: list) -> str:
    """
    Converts path components to a dot-separated string

    :param path: path components
    :returns: dot separated path string
    """
    return '.'.join(path)


NodeInfo = namedtuple('NodeInfo', ['answer_type', 'tag'])

def _load_node_path_map(session, form_id) -> dict:
    """
    Loads and constructs a information map used for transforming nested object nodes

    :param session: SQLAlchemy session
    :param form_id: Form.id
    :returns: dictionary of node path strings to NodeInfo instances
    """
    def _node_info(node: dict, path: list):
        answer_type = node.get('answerType')
        if answer_type:
            # schema_path, answerType, tag
            yield _make_path_str(path), constants.AnswerType[answer_type], node.get('tag')

    def _children(node: dict, path: list):
        node_slug = node.get('slug')
        children = node.get('children')
        if node_slug and children:
            for child in children:
                child_slug = child.get('slug')
                if child_slug:
                    yield path + [child_slug], child

    form_schema = session.query(models.Form.schema).filter_by(id=form_id).scalar()
    return {
        path: NodeInfo(answer_type, tag)
        for path, answer_type, tag in map_nested(form_schema, _node_info, _children)
    }


def get_node_path_map_cache(session):
    """
    Returns an LRU cached function which provides a node path map for a form

    :param session: SQLAlchemy session
    :return: function which provides a node path map (see _make_node_path_map) when passed a FormSchema.id
    """

    # NOTE: This wrapped partial function is the equivalent of defining an inner function like so:
    #
    # @functools.lru_cache(maxsize=NODE_PATH_CACHE_SIZE)
    # def _get_node_path_map(form_id):
    #     return _load_node_path_map(session, form_id)

    _get_node_path_map = functools.partial(_load_node_path_map, session)
    cached_wrapper = functools.lru_cache(maxsize=NODE_PATH_CACHE_SIZE)

    return cached_wrapper(_get_node_path_map)


def _make_output_dictionary(answer_type=None, **kwargs):
    return {
        'answer_type': answer_type.name,
        **kwargs
    }


def _transform_submission(f_get_node_path_map,
                          submission: models.Submission,
                          processed_on:datetime.datetime,
                          to_dict:bool):
    common_kwargs = {
        'processed_on': processed_on,
        'form_id': submission.form_id,

        'form_name': submission.form.name,

        'submission_id': submission.id,
        'submission_created': submission.date_created,

        # by default, accessing the 'user' relationship property will require a separate query to the 'users' table
        # if there is a cache miss.  This can be prevented with the 'joined_load' in the extractor
        'user_id': submission.user.id,
        'user_full_name': submission.user.full_name,
    }
    node_map = f_get_node_path_map(submission.form_id)
    if to_dict:
        output_mapper = _make_output_dictionary
    else:
        output_mapper = models.ResponseEvent

    for path, answer in _flatten_responses(submission.responses, node_map):
        yield output_mapper(
            schema_path=path,
            value=answer['value'],
            tag=answer['tag'],
            answer_type=answer['answer_type'],
            **common_kwargs
        )


def _flatten_responses(responses: dict, node_map: dict):
    def _answers(node: dict, path: list):
        for k, v in node.items():
            if not isinstance(v, (dict, list,)):
                path_str = _make_path_str(path + [k])
                node_info = node_map.get(path_str)
                if node_info:
                    # NOTE: for this workshop, we assume all date answer types are properly formatted
                    if node_info.answer_type == constants.AnswerType.boolean:
                        str_value = 'true' if v else 'false'
                    else:
                        str_value = str(v)
                    yield path_str, {'answer_type': node_info.answer_type, 'value': str_value, 'tag': node_info.tag}

    def _children(node: dict, path: list):
        for k, v in node.items():
            if isinstance(v, dict):
                yield path + [k], v

    yield from map_nested(responses, _answers, _children)
