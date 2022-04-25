import datetime

from dateutil.relativedelta import relativedelta
from fiql_parser import Constraint, Expression, Operator, parse_str_to_expression


def transform_year_id(year_id, selector):
    if selector == "yearid":
        return year_id
    elif selector == "termid":
        return year_id * 100
    elif "date" in selector:
        academic_year = year_id + 1990
        return datetime.date(academic_year, 7, 1)
    elif selector[-2:] == "id":
        return year_id * 100000
    else:
        return None


def get_constraint_rules(selector, year_id=None, is_historical=False):
    if selector == "yearid":
        return {"step_size": 1, "stop": 10}
    elif selector == "termid":
        term_id = transform_year_id(year_id, "termid")
        return {"step_size": 100, "stop": -term_id}
    elif "date" in selector:
        return {"step_size": relativedelta(years=1), "stop": datetime.date(2000, 7, 1)}
    elif is_historical:
        return {"step_size": 10000, "stop": 0}
    else:
        return {"step_size": None, "stop": 0}


def get_constraint_values(selector, value, step_size):
    if selector == "termid" and value < 0:
        step_size = step_size * -1
    elif "date" in selector and type(value) is str:
        value = datetime.datetime.strptime(value, "%Y-%m-%d").date()

    if step_size:
        return {"start_value": value, "end_value": (value + step_size)}
    else:
        return {"start_value": value, "end_value": None}


def get_query_expression(selector, start_value, end_value):
    query_expression = Expression()
    if type(start_value) is int and start_value < 0:
        query_expression.add_element(Constraint(selector, "=gt=", str(end_value)))
        query_expression.add_element(Operator(";"))
        query_expression.add_element(Constraint(selector, "=le=", str(start_value)))
    elif end_value is None:
        query_expression.add_element(Constraint(selector, "=ge=", str(start_value)))
    else:
        query_expression.add_element(Constraint(selector, "=ge=", str(start_value)))
        query_expression.add_element(Operator(";"))
        query_expression.add_element(Constraint(selector, "=lt=", str(end_value)))
    return str(query_expression)


def generate_historical_queries(year_id, selector, max_value=None):
    # transform yearid to constraint value
    if not max_value:
        max_value = transform_year_id(year_id, selector)

    # get step and stoppage critera for constraint type
    constraint_rules = get_constraint_rules(selector, year_id, is_historical=True)
    stop_value = constraint_rules["stop"]
    step_size = constraint_rules["step_size"]

    # generate probing queries
    working_value = max_value
    probing_query_expressions = []
    while working_value >= stop_value:
        constraint_values = get_constraint_values(selector, working_value, step_size)
        query_expression = get_query_expression(selector, **constraint_values)
        probing_query_expressions.append(query_expression)
        working_value = working_value - step_size
    return probing_query_expressions


def parse_fiql_selector(query_string):
    # parse query string to get selector
    query_expression = parse_str_to_expression(query_string)
    query_constraint = query_expression.elements[0]
    return query_constraint.selector
