import datetime
import functools
import logging
from collections import namedtuple

from app import models, constants
from app.util.timestamps import utc_now

LOGGER = logging.getLogger(__name__)
NODE_PATH_CACHE_SIZE = 16  # should be a power of 2


NodeInfo = namedtuple('NodeInfo', ['answer_type', 'tag'])


def map_nested(node:dict, gen_items, gen_children):
    """
    Transforms a nested python dictionary into a generator of items

    :param obj: current object node to transform
    :param path: path components
    :param gen_items: generator to extract items from node
        called with (node, path)
    :param gen_children: generator to extract children from node
        called with (node, path)
    """
    return _recursive_map_nested(node, [], gen_items, gen_children)


def _recursive_map_nested(node:dict, path: list, gen_items, gen_children):
    """
    NOTE: This is the core traversal algorithm (recursive depth first search with pre-order traversal)

    Recursive:  The same function is called on nested subtrees

    Pre-order traversal: Inspect a node's items first *before* considering its children

    Depth first search: When you recurse, explore as far down a child's tree before backtracking
    """
    if not node:
        return

    yield from gen_items(node, path)

    for child_path, child_node in gen_children(node, path):
        yield from _recursive_map_nested(child_node, child_path, gen_items, gen_children)


def _make_path_str(path: list) -> str:
    """
    Converts a path component list to a dot-separated string

    :param path: path components
    :returns: dot separated path string
    """
    return '.'.join(path)


def _load_node_path_map(session, form_id) -> dict:
    """
    Loads and constructs a information map used for transforming nested object nodes

    NOTE: This is the first application of our generic map_nested() routine

    :param session: SQLAlchemy session
    :param form_id: Form.id
    :returns: dictionary of node path strings to NodeInfo instances
    """

    # we use inner methods (closures) here rather than partial methods just to better
    # because argument freezing is not required and it also shows that that the complexity
    # lies in your function arguments to map_nested() and not the traversal algorithm itself

    def _node_info(node: dict, path: list):
        """
        for node's with answer types, just extract one item
        -> (schema_path, answerType, tag)
        """
        answer_type = node.get('answerType')
        if answer_type:
            yield _make_path_str(path), constants.AnswerType[answer_type], node.get('tag')

    def _children(node: dict, path: list):
        """
        schema's children are in the item keyed as 'children'

        each child is uniquely identified by the 'slug'
        so the 'slug' is appended to the end of the current node's path
        """
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


def _make_output_dictionary(answer_type=None, **kwargs):
    """
    Used only if we watch to return pure python dictonaries rather than
    ResponseEvent model instances
    """
    return {
        'answer_type': answer_type.name,
        **kwargs
    }


def _transform_submission(f_get_node_path_map,
                          submission: models.Submission,
                          processed_on:datetime.datetime,
                          to_dict:bool):
    """
    This is a second application of map_nested()

    Instead of a schema, we traverse the *submission* nested structure instead.  Items and children
    are structured differently in the response because each key in the tree is its own node.  Nested child
    nodes are now any dictionary value found in the tree.

    Here we don't use inner methods.  Instead, we use partial functions just once to freeze the node path map
    for items generator that extracts individual answers.  Both functions are immediately following this function.
    """

    # these kwargs are the same for all ResponseEvents.  so just construct it once
    common_kwargs = {
        'processed_on': processed_on,
        'form_id': submission.form_id,

        'form_name': submission.form.name,

        'submission_id': submission.id,
        'submission_created': submission.date_created,

        # by default, accessing the 'user' relationship property will require a separate query to the 'users' table
        # if it is not already in the SQLAlchemy session.
        #
        # This can be prevented with the 'joined_load' option in the extractor
        'user_id': submission.user.id,
        'user_full_name': submission.user.full_name,
    }

    # setup the generator arguments for map nested
    node_map = f_get_node_path_map(submission.form_id)
    f_extract_answers = functools.partial(_extract_answers, node_map=node_map)

    # final conversion is usually a ResponseEvent.  however, we allow generating plain dictionaries
    # if specified for performance purposes
    if to_dict:
        output_mapper = _make_output_dictionary
    else:
        output_mapper = models.ResponseEvent

    for path, answer in map_nested(submission.responses, f_extract_answers, _dict_children):
        yield output_mapper(
            schema_path=path,
            value=answer['value'],
            tag=answer['tag'],
            answer_type=answer['answer_type'],
            **common_kwargs
        )


def _dict_children(node: dict, path: list):
    """ extracts child trees from a plain python dictionary """
    for k, v in node.items():
        if isinstance(v, dict):
            yield path + [k], v


def _extract_answers(node: dict, path: list, node_map=None):
    """ converts scalar dictionary items to response event arguments reflecting answers """
    assert node_map

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
