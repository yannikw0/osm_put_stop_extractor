import osmium
import pandas as pd
import logging


class PublicTransportStopExtractor:
    def __init__(self, osm_file):
        self.osm_file = osm_file  # Store the file path
        # Storage for extracted data
        self.stoparea_elems = {}  # OSM elements tagged in a stop_area relation with the name of the stop_area
        self.putline_elems = {}  # OSM elements tagged in a route relation with info of the route (service type)
        self.relation_way_node_refs = set()  # PuT nodes referenced by relations or ways (key: node ID, value: list of IDs)
        self.relation_way_refs = {}  # PuT ways referenced by relations (key: way ID, value: list of IDs). Need to be a dict with reference to the original relation
        self.nodes_coords = {}  # Coordinates of nodes
        self.stop_data = {}  # Final stop data (id, name, type, centroid)

    class RelationHandler(osmium.SimpleHandler):
        def __init__(self, parent):
            super().__init__()
            self.parent = parent
            # prioritization of different kinds of train services
            self.prioritization_services = {
                'high_speed': 1,
                'long_distance': 2,
                'regional': 3,
                'commuter': 4,
                'night': 5,
                'tourism': 6,
                'car': 7,
                'car_shuttle': 8,
                'event': 9,
            }

        def relation(self, r):
            put_tag = r.tags.get('public_transport')
            # Process relations tagged as put stop
            if put_tag in ['platform', 'stop_position']:
                node_refs = []
                way_refs = []

                # Collect node and way references
                for member in r.members:
                    if member.type == 'n':
                        # TODO: This could also be done with the location information directly through locations=True!
                        self.parent.relation_way_node_refs.add(member.ref)
                        node_refs.append(member.ref)
                    elif member.type == 'w':
                        if member.ref not in self.parent.relation_way_refs:
                            self.parent.relation_way_refs[member.ref] = []
                        self.parent.relation_way_refs[member.ref].append(r.id)
                        way_refs.append(member.ref)

                # Store stop data for relation
                relevant_service_tags = {key: value for key, value in r.tags if key in ['train', 'subway', 'light_rail', 'tram', 'railway', 'bus', 'highway']}
                general_type, specific_type = self.parent.check_service_from_element_tags(relevant_service_tags)
                self.parent.stop_data[r.id] = {
                    'name': r.tags.get('name', 'N/A'),
                    'object_type': 'relation',
                    'public_transport': put_tag,
                    'service_type': general_type,
                    'route_type': specific_type,
                    'node_refs': node_refs,
                    'way_refs': way_refs,
                }

            # Process relations tagged as stop_area
            if r.tags.get('public_transport') == 'stop_area':
                for member in r.members:
                    stop_area_name = r.tags.get('name', 'N/A')
                    self.parent.stoparea_elems[member.ref] = stop_area_name

            # Process relations tagged as PuT line
            if 'route' in r.tags:
                route_type = r.tags['route']
                # assign priority to train stops
                if route_type == 'train' and 'service' in r.tags:
                    service_type = r.tags['service']
                    if service_type in self.prioritization_services:
                        priority = self.prioritization_services[service_type]
                    else:
                        raise ValueError(f'train service_type: {service_type} not in prioritization list.')
                else:
                    service_type = ''
                    priority = 10
                for member in r.members:
                    if member.role == 'stop' or member.role == 'platform':
                        # Check if the node is already stored and if the new service has higher priority
                        if member.ref not in self.parent.putline_elems or self.parent.putline_elems[member.ref]['service_priority'] > priority:
                            self.parent.putline_elems[member.ref] = {
                                "route_type": route_type,
                                "service_type": service_type,
                                "service_priority": priority
                            }

    class WayHandler(osmium.SimpleHandler):
        def __init__(self, parent):
            super().__init__()
            self.parent = parent

        def way(self, w):
            put_tag = w.tags.get('public_transport')
            railway_tag = w.tags.get('railway')
            # Process ways that are tagged as public_transport stop OR station OR are part of a relevant relation
            if put_tag in ['platform', 'stop_position', 'station'] or railway_tag in ['station', 'halt', 'tram_stop'] or w.id in self.parent.relation_way_refs:
                node_refs = []
                for n in w.nodes:
                    self.parent.relation_way_node_refs.add(n.ref)
                    node_refs.append(n.ref)
                # Store stop data for the way, whether it's tagged itself or belongs to a relation
                #1. Check if tagged with public_transport
                if put_tag in ['platform', 'stop_position']:
                    relevant_service_tags = {key: value for key, value in w.tags if key in ['train', 'subway', 'light_rail', 'tram', 'railway', 'bus', 'highway']}
                    general_type, specific_type = self.parent.check_service_from_element_tags(relevant_service_tags)
                    self.parent.stop_data[w.id] = {
                        'name': w.tags.get('name', 'N/A'),
                        'object_type': 'way',
                        'public_transport': put_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'node_refs': node_refs,
                    }
                # Additionally check if tagged with railway (EBO vs BOStrab), if already exists overwrites with same info + adds railway tag
                if railway_tag in ['station', 'halt', 'tram_stop']:
                    relevant_service_tags = {key: value for key, value in w.tags if key in ['station', 'railway']}
                    general_type, specific_type = self.parent.check_station_service_from_element_tags(relevant_service_tags)
                    # Store stop data for the way, whether it's tagged itself or belongs to a relation
                    self.parent.stop_data[w.id] = {
                        'name': w.tags.get('name', 'N/A'),
                        'object_type': 'way',
                        'railway': railway_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'node_refs': node_refs,
                    }
                elif put_tag == 'station':
                    relevant_service_tags = {key: value for key, value in w.tags if key in ['station', 'railway', 'bus']}
                    general_type, specific_type = self.parent.check_station_service_from_element_tags(relevant_service_tags)
                    # Store stop data for the way, whether it's tagged itself or belongs to a relation
                    self.parent.stop_data[w.id] = {
                        'name': w.tags.get('name', 'N/A'),
                        'object_type': 'way',
                        'railway': railway_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'node_refs': node_refs,
                    }
                # Process ways that are part of a relevant relation (not elif because can be in both!)
                if w.id in self.parent.relation_way_refs:
                    node_refs = [n.ref for n in w.nodes]
                    if w.id in self.parent.relation_way_refs:
                        for r_id in self.parent.relation_way_refs[w.id]:
                            self.parent.stop_data[r_id]['node_refs'].extend(node_refs)

    class NodeHandler(osmium.SimpleHandler):
        def __init__(self, parent):
            super().__init__()
            self.parent = parent

        def node(self, n):
            put_tag = n.tags.get('public_transport')
            railway_tag = n.tags.get('railway')
            # Process nodes that are either part of relations or ways, or are tagged independently as stops
            if put_tag in ['platform', 'stop_position', 'station'] or railway_tag in ['station', 'halt', 'tram_stop'] or n.id in self.parent.relation_way_node_refs:
                # Store the coordinates of the node
                self.parent.nodes_coords[n.id] = (n.location.lat, n.location.lon)

                # Directly store stop data for independently tagged nodes
                # 1. Nodes tagged with public_transport
                if n.tags.get('public_transport') in ['platform', 'stop_position']:
                    relevant_service_tags = {key: value for key, value in n.tags if key in ['train', 'subway', 'light_rail', 'tram', 'railway', 'bus', 'highway']}
                    general_type, specific_type = self.parent.check_service_from_element_tags(relevant_service_tags)
                    self.parent.stop_data[n.id] = {
                        'name': n.tags.get('name', 'N/A'),
                        'object_type': 'node',
                        'public_transport': put_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'lat': n.location.lat,
                        'lon': n.location.lon
                    }
                # 2. Alternatively check if tagged with railway (EBO vs BOStrab), if already exists overwrites with same info + adds railway tag
                if railway_tag in ['station', 'halt', 'tram_stop']:
                    relevant_service_tags = {key: value for key, value in n.tags if key in ['station', 'railway']}
                    general_type, specific_type = self.parent.check_station_service_from_element_tags(relevant_service_tags)
                    # Store stop data for the way, whether it's tagged itself or belongs to a relation
                    self.parent.stop_data[n.id] = {
                        'name': n.tags.get('name', 'N/A'),
                        'object_type': 'node',
                        'railway': railway_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'lat': n.location.lat,
                        'lon': n.location.lon
                    }
                elif put_tag =='station':
                    relevant_service_tags = {key: value for key, value in n.tags if key in ['station', 'railway', 'bus']}
                    general_type, specific_type = self.parent.check_station_service_from_element_tags(relevant_service_tags)
                    # Store stop data for the way, whether it's tagged itself or belongs to a relation
                    self.parent.stop_data[n.id] = {
                        'name': n.tags.get('name', 'N/A'),
                        'object_type': 'node',
                        'railway': railway_tag,
                        'service_type': general_type,
                        'route_type': specific_type,
                        'lat': n.location.lat,
                        'lon': n.location.lon
                    }

    def process_relations(self):
        """Run the relation handler on the OSM file."""
        relation_handler = self.RelationHandler(self)
        relation_handler.apply_file(self.osm_file)  # Use the stored file path

    def process_ways(self):
        """Run the way handler on the OSM file."""
        way_handler = self.WayHandler(self)
        way_handler.apply_file(self.osm_file, locations=True)  # Use the stored file path

    def process_nodes(self):
        """Run the node handler on the OSM file."""
        node_handler = self.NodeHandler(self)
        node_handler.apply_file(self.osm_file, locations=True)  # Use the stored file path

    @staticmethod
    def check_service_from_element_tags(tags):
        # tag_lookup = {
        #     'train': 'train',
        #     'subway': 'subway',
        #     'light_rail': 'light_rail',
        #     'tram': 'tram',
        #     'railway': 'railway_platform',
        #     'bus': 'bus',
        #     'highway': 'highway_platform'
        # }
        tag_lookup = {
            'train': ['rail', 'train'],
            'subway': ['rail', 'subway'],
            'light_rail': ['rail', 'light_rail'],
            'tram': ['rail', 'tram'],
            'bus': ['bus', 'bus']
        }
        #Vsys-Check
        for tag, result in tag_lookup.items():
            if tags.get(tag) == 'yes':
                return result[0], result[1]
        #bus_stop check
        if tags.get('highway') == 'bus_stop':
            return 'bus', 'bus'
        #platform check
        for tag, result in {'railway': ['rail', 'railway_platform'], 'highway': ['bus', 'highway_platform']}.items():
            if tags.get(tag) == 'platform':
                return result[0], result[1]

        return 'unknown', 'unknown'

    @staticmethod
    def check_station_service_from_element_tags(tags):
        if tags.get('station') in ['train', 'subway', 'light_rail', 'monorail', 'funicular']:
            return 'rail', tags.get('station')
        elif tags.get('bus') == 'yes':
            return 'bus', 'bus'
        return 'rail', tags.get('railway')

    # TODO: This is not hte actual centroid, just the mean of all involved vertices. Most likely within the area but not at the real centroid! See calculate_centroid_vectorized for possible np function to calculate from locations
    def compute_centroids(self):
        """ Compute centroids for ways and relations after node processing. """
        for stop_id, stop_info in self.stop_data.items():
            if stop_info['object_type'] == 'relation' or stop_info['object_type'] == 'way':
                node_coords = []

                # Collect node coordinates from relation's or way's node references
                for node_ref in stop_info.get('node_refs', []):
                    if node_ref in self.nodes_coords:
                        node_coords.append(self.nodes_coords[node_ref])

                # for way_ref in stop_info.get('way_refs', []):
                #     if way_ref in self.way_node_refs:
                #         for node_ref in self.way_node_refs[way_ref]:
                #             if node_ref in self.nodes_coords:
                #                 node_coords.append(self.nodes_coords[node_ref])

                # Calculate the centroid for this relation or way
                if node_coords:
                    centroid_lat, centroid_lon = self.compute_centroid(node_coords)
                    stop_info['lat'] = centroid_lat
                    stop_info['lon'] = centroid_lon
    @staticmethod
    def compute_centroid(coords):
        """ Compute the centroid of a list of (lat, lon) coordinates. """
        if not coords:
            return None, None
        lat_sum = sum(coord[0] for coord in coords)
        lon_sum = sum(coord[1] for coord in coords)
        count = len(coords)
        return (lat_sum / count, lon_sum / count)

    # def calculate_centroid_vectorized(nodes):
    #     # Extract coordinates into numpy arrays for efficient vector operations
    #     coords = np.array([(node.location.x, node.location.y) for node in nodes])
    #
    #     x = coords[:, 0]
    #     y = coords[:, 1]
    #
    #     # Shift the coordinates for vectorized cross-product calculation
    #     x_shift = np.roll(x, -1)
    #     y_shift = np.roll(y, -1)
    #
    #     # Compute area (A) and the centroids
    #     cross = x * y_shift - x_shift * y
    #     A = np.sum(cross) / 2.0
    #     Cx = np.sum((x + x_shift) * cross) / (6.0 * A)
    #     Cy = np.sum((y + y_shift) * cross) / (6.0 * A)
    #
    #     return Cx, Cy

    def add_info_stoparea_putline(self):
        # Iterate over the dictionary and update names and putline info
        # lookup the stoparea name based on node_id and replace if not 'N/A'
        for obj_id, info in self.stop_data.items():
            if obj_id in self.stoparea_elems and self.stoparea_elems[obj_id] != 'N/A':
                info['stoparea_name'] = self.stoparea_elems[obj_id]
            else:
                info['stoparea_name'] = info['name']
            if obj_id in self.putline_elems:
                info["is_in_route"] = True
                info["route_type"] = self.putline_elems[obj_id]["route_type"]
                info["service_type"] = self.putline_elems[obj_id]["service_type"]
                info["service_priority"] = self.putline_elems[obj_id]["service_priority"]
            else:
                info["is_in_route"] = False

    def get_results(self):
        """ Return the processed stop data (including centroids). """
        # Convert to DataFrame
        results_df = pd.DataFrame.from_dict(self.stop_data, orient='index')
        return results_df


if __name__ == '__main__':
    # Configure logger
    logging.basicConfig(
        level=logging.INFO,  # Set the log level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Create a logger
    logger = logging.getLogger(__name__)

    osm_file_path = '20240806_Stuttgart30kmBoundingBox.osm.pbf'
    # Pass the file path directly when creating an instance of the class
    extractor = PublicTransportStopExtractor(osm_file_path)

    # Process the OSM file in sequence:

    # 1. Process relations:
    #    - Relations reference ways and nodes.
    #    - We gather node/way references from relevant relations (tagged as public_transport=platform/stop_position).
    #    - Also gather relations tagged as public_transport = stop_area to retrieve their names for later aggregation (in QGIS)
    #    - extract PuT lines (tagged as put route in OSM) to extract the info whether any of the elements is used in an existing line
    #      The service type can also be extracted as well which is necessary especially for trains
    logger.info('Processing relations...')
    extractor.process_relations()

    # 2. Process ways:
    #    - Ways consist of multiple nodes but contain no coordinates.
    #    - We collect node references from ways (tagged as public_transport=platform/stop_position) and those in relations.
    logger.info('Processing ways...')
    extractor.process_ways()

    # 3. Process nodes:
    #    - Nodes are the only objects containing geographic coordinates.
    #    - We extract coordinates for nodes that are part of the relevant ways or relations.
    logger.info('Processing nodes...')
    extractor.process_nodes()

    logger.info('Postprocessing data...')
    # After processing, compute centroids for relations and ways using node coordinates.
    extractor.compute_centroids()

    # If possible add the name from the stoparea relations for later aggragation (in QGIS)
    extractor.add_info_stoparea_putline()

    # Column descriptions for results_df:
    # 'name': Name of the public transport stop or station. If not available, defaults to 'N/A'.
    # 'object_type': Type of OSM object (either 'node', 'way', or 'relation').
    # 'public_transport': Tag indicating if the stop is a 'platform' or 'stop_position' (derived from OSM tags).
    # 'service_type': Type of public transport service associated with the stop (e.g., 'train', 'subway', 'bus', etc.).
    # 'node_refs': List of node IDs referenced by the relation or way (for ways or relations that involve multiple nodes).
    # 'way_refs': List of way IDs referenced by the relation (for relations that involve multiple ways).
    # 'lat': Latitude coordinate of the stop (for individual nodes, or centroid for ways and relations).
    # 'lon': Longitude coordinate of the stop (for individual nodes, or centroid for ways and relations).
    # 'stoparea_name': Name of the stop area if the stop belongs to a stop area relation (else, same as 'name').
    # 'is_in_route': Boolean indicating if the stop is part of a public transport route relation (True if in a route, False otherwise).
    # 'route_type': Type of the route the stop belongs to (e.g., 'train', 'bus', etc.), if applicable.
    # 'service_priority': Priority of the public transport service type (for train routes, based on service type like high-speed, regional, etc.).
    results_df = extractor.get_results()

    # Write the DataFrame to a CSV file
    csv_file_raw = "S30_stops_put_stops_processed.csv"
    results_df.to_csv(csv_file_raw, index=False)
    logger.info('CSV file with extracted PuT data created successfully.')