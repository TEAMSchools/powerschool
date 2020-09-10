from fiql_parser import Expression, Constraint, Operator, parse_str_to_expression

from dateutil.relativedelta import relativedelta
import datetime


def transform_yearid(yearid, selector):
    if selector == "yearid":
        return yearid
    elif selector == "termid":
        return yearid * 100
    elif "date" in selector:
        academic_year = yearid + 1990
        return datetime.date(academic_year, 7, 1)


def get_constraint_rules(selector, yearid=None):
    if selector == "yearid":
        return {"step_size": 1, "stop": 10}
    elif selector == "termid":
        termid = transform_yearid(yearid, "termid")
        return {"step_size": 100, "stop": -termid}
    elif "date" in selector:
        return {"step_size": relativedelta(years=1), "stop": datetime.date(2000, 7, 1)}
    else:
        return {"step_size": 10000, "stop": 1}


def get_constraint_values(selector, arg_value, step_size):
    if selector == "yearid":
        arg_next = arg_value + step_size
    elif selector == "termid" and arg_value < 0:
        arg_next = arg_value - step_size
    elif selector == "termid" and arg_value >= 0:
        arg_next = arg_value + step_size
    elif "date" in selector and type(arg_value) is str:
        arg_value = datetime.datetime.strptime(arg_value, "%Y-%m-%d").date()
        arg_next = arg_value + step_size
    elif "date" in selector and isinstance(arg_value, datetime.date):
        arg_next = arg_value + step_size
    else:
        arg_next = None
    return {"start": arg_value, "end": arg_next}


def get_query_expression(selector, start, end):
    query_expression = Expression()
    if type(start) is int and start < 0:
        query_expression.add_element(Constraint(selector, "=gt=", str(end)))
        query_expression.add_element(Operator(";"))
        query_expression.add_element(Constraint(selector, "=le=", str(start)))
    elif end is None:
        query_expression.add_element(Constraint(selector, "=ge=", str(start)))
    else:
        query_expression.add_element(Constraint(selector, "=ge=", str(start)))
        query_expression.add_element(Operator(";"))
        query_expression.add_element(Constraint(selector, "=lt=", str(end)))
    return str(query_expression)


def generate_historical_queries(current_yearid, query_constraint_selector):
    ## transform yearid to constraint value
    max_constraint_value = transform_yearid(current_yearid, query_constraint_selector)

    ## get step and stoppage critera for constraint type
    constraint_rules = get_constraint_rules(query_constraint_selector, current_yearid)
    stop_constraint_value = constraint_rules["stop"]
    constraint_step_size = constraint_rules["step_size"]

    ## generate probing queries
    working_constraint_value = max_constraint_value
    probing_query_expressions = []
    while working_constraint_value >= stop_constraint_value:
        constraint_values = get_constraint_values(
            query_constraint_selector, working_constraint_value, constraint_step_size
        )
        query_expression = get_query_expression(
            query_constraint_selector, **constraint_values
        )
        probing_query_expressions.append(query_expression)
        working_constraint_value = (
            working_constraint_value - constraint_rules["step_size"]
        )
    return probing_query_expressions


def parse_fiql_selector(query_string):
    # parse query string to get selector
    query_expression = parse_str_to_expression(query_string)
    query_constraint = query_expression.elements[0]
    return query_constraint.selector
