class SinkRow:
    sequence_number = None
    timestamp = None
    source_addr = None
    destination_addr = None
    apci = None
    priority = None
    flag_communication = None
    flag_read = None
    flag_write = None
    flag_transmit = None
    flag_refresh = None
    flag_read_at_init = None
    repeated = None
    hop_count = None
    payload = None
    payload_length = None
    raw_package = None
    is_manipulated = None
    attack_type_id = None

# sink_row = {'sequence_number': None,
#             'timestamp': None,
#             'source_addr': None,
#             'destination_addr': None,
#             'apci': None,
#             'priority': None,
#             'flag_communication': None,
#             'flag_read': None,
#             'flag_write': None,
#             'flag_transmit': None,
#             'flag_refresh': None,
#             'flag_read_at_init': None,
#             'repeated': None,
#             'hop_count': None,
#             'payload': None,
#             'payload_length': None,
#             'raw_package': None,
#             'is_manipulated': None,
#             'attack_type_id': None,
#             }