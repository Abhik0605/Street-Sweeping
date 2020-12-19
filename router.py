from __future__ import print_function
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import psycopg2
import ast
from psycopg2 import sql
import json


def read_matrix_from_json():
    with open('distance_matrix.json') as f:
        matrix = json.load(f)

    return matrix['matrix']


def create_data_madel(input):
    data = {}
    data['distance_matrix'] = input
    data['num_vehicles'] = 1
    data['depot'] = 0
    return data


def add_distance_callback(routing, manager, data):
    def distance_callback(from_index, to_index):
        """Returns the distance between the two nodes."""
        # Convert from routing variable Index to distance matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data['distance_matrix'][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)

    dimension_name = 'Distance'
    routing.AddDimension(
        transit_callback_index,
        0,  # no slack
        100_000_000,  # vehicle maximum travel distance
        True,  # start cumul to zero
        dimension_name)
    distance_dimension = routing.GetDimensionOrDie(dimension_name)


    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)


def get_routes(data, solution, routing, manager):
  """Get vehicle routes from a solution and store them in an array."""
  # Get vehicle routes and store them in a two dimensional array whose
  # i,j entry is the jth location visited by vehicle i along its route.
  routes = []
  for vehicle_id in range(data['num_vehicles']):
    foo = []
    index = routing.Start(vehicle_id)
    foo.append(index)

    while not routing.IsEnd(index):
        index = solution.Value(routing.NextVar(index))
        foo.append(index)
        foo[0] = 0

    foo.pop(-1)
    foo.append(-1)

    routes.append(foo)

  return routes


def routing_solver(input, time=None, solution_limit=None, lns_time=None ,penalty=None, log_search=bool, first_solution_method=None ,second_solution_method=None):
    """Entry point of the program."""
    # Instantiate the data problem.
    data = create_data_madel(input)

    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']),
                                           data['num_vehicles'], data['depot'])

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)

    add_distance_callback(routing, manager, data)

    if penalty != None:
        for node in range(1, len(data['distance_matrix'])):
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)


    search_parameters = pywrapcp.DefaultRoutingSearchParameters()

    if solution_limit != None:
        search_parameters.solution_limit = solution_limit

    if time != None:
        search_parameters.time_limit.seconds = time

    if lns_time != None:
        search_parameters.lns_time_limit.seconds = lns_time

    # Setting first solution heuristic.

    if first_solution_method == 'PATH_CHEAPEST_ARC':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
    elif first_solution_method == 'SAVINGS':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.SAVINGS)
    elif first_solution_method == 'SWEEP':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.SWEEP)
    elif first_solution_method == 'CHRISTOFIDES':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.CHRISTOFIDES)
    elif first_solution_method == 'BEST_INSERTION':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.BEST_INSERTION)
    elif first_solution_method == 'PARALLEL_CHEAPEST_INSERTION':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION)
    elif first_solution_method == 'FIRST_UNBOUND_MIN_VALUE':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.FIRST_UNBOUND_MIN_VALUE)
    elif first_solution_method == 'LOCAL_CHEAPEST_ARC':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_ARC)
    elif first_solution_method == 'GLOBAL_CHEAPEST_ARC':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.GLOBAL_CHEAPEST_ARC)
    elif first_solution_method == 'AUTOMATIC':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.AUTOMATIC)
    elif first_solution_method == 'LOCAL_CHEAPEST_INSERTION':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.LOCAL_CHEAPEST_INSERTION)
    elif first_solution_method == 'ALL_UNPERFORMED':
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.ALL_UNPERFORMED)
    elif first_solution_method == 'GREEDY_DESCENT':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GREEDY_DESCENT)

    if second_solution_method == 'TABU_SEARCH':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.TABU_SEARCH)
    elif second_solution_method == 'GUIDED_LOCAL_SEARCH':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    elif second_solution_method == 'SIMULATED_ANNEALING':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.SIMULATED_ANNEALING)
    elif second_solution_method == 'OBJECTIVE_TABU_SEARCH':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.OBJECTIVE_TABU_SEARCH)
    elif second_solution_method == 'GREEDY_DESCENT':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GREEDY_DESCENT)
    elif second_solution_method == 'AUTOMATIC':
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC)


    if log_search == True:
        search_parameters.log_search = True

    # Solve the problem.
    solution = routing.SolveWithParameters(search_parameters)

    # Print solution on console.
    routes = get_routes(data, solution, routing, manager)

    if solution:
        return routes


def db_connect(host=None, database=None, user=None, password=None):
    global con
    con = psycopg2.connect(
        host = f"{host}",
        database = f"{database}",
        user = f"{user}",
        password = f"{password}"
        )


def get_new_routes(routes):
    db_connect(host="localhost", database="chicago", user="postgres", password="password")
    cur = con.cursor()

    cur.execute(f"select * from distance_matrix where start_vid = {1};")
    items = cur.fetchall()
    items = sorted(items, key=lambda x: x[1])

    routing_indices = []

    for i in items:
        routing_indices.append(i[1])

    routing_indices.insert(0,1)
    # routing_indices.insert(0,0)
    routing_indices.append(-1)

    new_routes = []
    for i in routes[0]:
        new_routes.append(routing_indices[i])

    new_routes.pop(-1)

    with open("routes.txt", "w") as f:
        f.write(f"{new_routes}")

    return new_routes


def sequence_to_table(sequence, table_name):
    db_connect(host="localhost", database="chicago", user="postgres", password="password")
    insert_query_string = """insert into {} (sweep,linkid,geom, geojson)
    select %s,%s,c.curb_geom, ST_AsGeoJSON(ST_Transform(c.curb_geom,4326)) as the_geom
    from curbs_v2_graph c
    where c.curbid =%s"""
    insert_query = sql.SQL(
                     insert_query_string
                          ).format(sql.Identifier(table_name))
    with con.cursor() as cur:
        cur.execute(
            sql.SQL(
                "Drop table if exists {}"
                   ).format(sql.Identifier(table_name)))
        cur.execute(
            sql.SQL("""
    CREATE TABLE {} (
       id serial, linkid integer,
       sweep boolean default TRUE,
       geom geometry(LINESTRING,4326),
       geojson text)
                   """).format(sql.Identifier(table_name)))
        geojson = []
        for idx, row in enumerate(sequence):
            # basic idea.  Save a table.
            # Each row has shape,sequence number
            sweep = 'TRUE'
            linkid = row
            cur.execute(
               insert_query,(sweep,linkid,linkid))

        cur.execute(f"SELECT geojson FROM routing_output ")
        x = cur.fetchall()
        # geojson.append(x)
    con.commit()

    return x


def get_geojson():
    db_connect(host="localhost", database="chicago", user="postgres", password="password")
    insert_query_string = """
                              select geojson from routing_output

                            """
    insert_query = sql.SQL(
                     insert_query_string
                          )



def get_distance_matrix():
    db_connect(host="localhost", database="chicago", user="postgres", password="password")

    cur = con.cursor()

    cur.execute(f"select * from distance_matrix where start_vid = {1};")
    items = cur.fetchall()
    items = sorted(items, key=lambda x: x[1])

    routing_indices = []

    for i in items:
        routing_indices.append(i[1])

    matrix = []

    for idx, i in enumerate(routing_indices):
        row = []
        cur.execute(f"select * from distance_matrix where start_vid = {i};")
        items = cur.fetchall()
        items = sorted(items, key=lambda x: x[1])
        if len(items) < len(routing_indices)-1:
            print(i)

        for j in items:
            row.append(int(j[2]))

        if idx != len(routing_indices)-1:
            row.insert(idx, 0)
        else:
            row.append(0)

        matrix.append(row)

    con.close()

    return matrix


input = read_matrix_from_json()

routes = get_new_routes(routing_solver(input,
                                       time=10,
                                       first_solution_method="PATH_CHEAPEST_ARC",
                                       second_solution_method="GUIDED_LOCAL_SEARCH"))

routing_output = "routing_output"

geojson = sequence_to_table(routes, routing_output)
# print(json.loads(geojson[0][0])["coordinates"][0])
# print(routes)
# print(json.loads(geojson[0][0])["coordinates"][:])
coords = []
for i in range(len(geojson)):
    coords.append(json.loads(geojson[i][0])["coordinates"][0])
    coords.append(json.loads(geojson[i][0])["coordinates"][1])


foo = {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": [],
          "geometry": {
            "type": "LineString",
            "coordinates": coords[:100]
          }
        }
      ]
       }

print(json.dumps(foo))
