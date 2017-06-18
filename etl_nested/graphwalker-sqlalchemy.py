import json

from app.models import BaseModel

from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.inspection import inspect


def get_vertex_key(cls_orm_model):
    return '{}.{}'.format(cls_orm_model.__module__, cls_orm_model.__name__)


def _r_extract_hierarchy(cls_orm_model, visited: set):
    # avoid cycles
    vertex_key = get_vertex_key(cls_orm_model)
    if vertex_key in visited:
        return
    else:
        visited.add(vertex_key)

    # extract the current vertex
    vertex = {
        'key': vertex_key,
        'name': cls_orm_model.__name__,
        'module': cls_orm_model.__module__,
        'relations': [],
    }

    # add edges for relationships
    try:
        mapper = inspect(cls_orm_model)
    except NoInspectionAvailable:
        mapper = None
    if mapper:
        for relation_name in mapper.relationships.keys():
            relation = mapper.relationships[relation_name]
            vertex['relations'].append({
                'source_name': relation_name,
                'source_columns': [c.name for c in relation.local_columns],
                'is_self_referential': relation._is_self_referential,
                'target': get_vertex_key(relation.argument),
                'taget_columns': [c.name for c in relation.remote_side],
                'back_reference_name': relation.backref,
                'type': relation.direction.name
            })

    # add edges for inheritance
    for child_class in cls_orm_model.__subclasses__():
        vertex['relations'].append({
            'target': get_vertex_key(child_class),
            'type': 'inheritance'
        })

    # output current vertex
    yield vertex_key, vertex

    # recurse
    for child_class in cls_orm_model.__subclasses__():
        yield from _r_extract_hierarchy(child_class, visited)


graph = {
    k: v for k,v in _r_extract_hierarchy(BaseModel, set())
}
print(json.dumps(graph))
