CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'dim_time',
  table_location => '/workspace/data/delta/gold/dim_time'
);
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'dim_road_segment',
  table_location => '/workspace/data/delta/gold/dim_road_segment'
);
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'dim_event_type',
  table_location => '/workspace/data/delta/gold/dim_event_type'
);
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'dim_vehicle',
  table_location => '/workspace/data/delta/gold/dim_vehicle'
);
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'fact_traffic_events',
  table_location => '/workspace/data/delta/gold/fact_traffic_events'
);
