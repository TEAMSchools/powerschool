from fiql_parser import Expression, Constraint, Operator, parse_str_to_expression

from dateutil.relativedelta import relativedelta
import datetime

def translate_yearid_to_selector(yearid, selector):
    if selector == 'yearid':
        return yearid
    elif selector == 'termid':
        return yearid * 100
    elif 'date' in selector:
        academic_year = yearid + 1990
        return datetime.date(academic_year, 7, 1)

def generate_constraint_rules(selector, yearid=None):
    if selector == 'yearid':
        return {'step_size': 1, 'stop_arg': 10}
    elif selector == 'termid':
        termid = translate_yearid_to_selector(yearid, 'termid')
        return {'step_size': 100, 'stop_arg': -termid}
    elif 'date' in selector:
        return {'step_size': relativedelta(years=1), 'stop_arg': datetime.date(2000, 7, 1)}

def generate_constraint_args(selector, arg, step_size):
    if selector == 'termid' and arg < 0:
        arg_next = arg - step_size
    else:
        arg_next = arg + step_size
    return {'start_arg': arg, 'stop_arg': arg_next}

def generate_query_expression(selector, start_arg, stop_arg):
    query_expression = Expression()
    if type(start_arg) is int and start_arg < 0:
        query_expression.add_element(Constraint(selector, '=le=', str(start_arg)))
        query_expression.add_element(Operator(';'))
        query_expression.add_element(Constraint(selector, '=gt=', str(stop_arg)))
    else:
        query_expression.add_element(Constraint(selector, '=ge=', str(start_arg)))
        query_expression.add_element(Operator(';'))
        query_expression.add_element(Constraint(selector, '=lt=', str(stop_arg)))
    return str(query_expression)

def parse_fiql(query_string):
    # parse query string to get selector
    query_expression = parse_str_to_expression(query_string)
    query_constraint = query_expression.elements[0]
    return query_constraint.selector

def generate_historical_queries(current_yearid, query_constraint_selector):
    ## translate yearid to constraint value
    max_constraint_arg = translate_yearid_to_selector(current_yearid, query_constraint_selector)

    ## get step and stoppage critera for constraint type
    constraint_rules = generate_constraint_rules(query_constraint_selector, current_yearid)
    constraint_stop_arg = constraint_rules['stop_arg']
    constraint_step_size = constraint_rules['step_size']

    ## generate probing queries
    working_constraint_arg = max_constraint_arg
    probing_query_expressions = []
    while working_constraint_arg >= constraint_stop_arg:
        constraint_args = generate_constraint_args(query_constraint_selector, working_constraint_arg, constraint_step_size)
        query_expression = generate_query_expression(query_constraint_selector, **constraint_args)
        probing_query_expressions.append(query_expression)
        working_constraint_arg = working_constraint_arg - constraint_rules['step_size']
    return probing_query_expressions
    