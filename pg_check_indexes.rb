require 'pg'
require 'optparse'

opts_db = {}
opts_sc = {}

option_parser =
  OptionParser.new do |cmd_options|
    cmd_options.on(
      '-d', '--database DBNAME', 'The database name to connect to'
    ) { |c| opts_db[:dbname] = c }
    cmd_options.on(
      '-u', '--user DBUSER', 'PostgreSQL user name'
    ) { |c| opts_db[:user] = c }
    cmd_options.on(
      '-h', '--hostname DBHOSTNAME', 'PostgreSQL hostname'
    ) { |c| opts_db[:host] = c }
    cmd_options.on(
      '-p', '--port DBPORT', 'PostgreSQL listen port'
    ) { |c| opts_db[:port] = c }
    cmd_options.on(
      '-t', '--table TABLENAME', 'Check only a selected table, use schema qualified names.'
    ) { |c| opts_sc[:tablename] = c }
    cmd_options.on(
      '-T', '--threshold MAX_TABLE_SIZE', 'in bytes, 0 for unlimited'
    ) { |c| opts_sc[:threshold] = c }
    cmd_options.on(
      '-s', '--stop-on-failure', 'If "y", execution will stop when corruption found'
    ) { |c| opts_sc[:stop_on_fail] = c }
  end
option_parser.parse!

def index_table_match(connection_object, index_info, do_stop)
  open_parentesis_counter = 0
  first_parentesis        = false
  string_position         = 0
  column_definition_start = 0
  column_definition_end   = 0
  where_position          = 0

  index_info['index_definition'].each_char do |each_letter|
    # This loop is a parser to find out the columns to test in each index.
    # While it's not the most pretty parser in the world, it works as
    # expected to extract the columns in a PostgreSQL index definition.

    if each_letter == '('
      open_parentesis_counter += 1
    elsif each_letter == ')'
      open_parentesis_counter -= 1
    end

    if open_parentesis_counter == 1 && !first_parentesis
      first_parentesis = true
      column_definition_start = string_position
    end

    if open_parentesis_counter == 0 && first_parentesis && column_definition_end == 0
      column_definition_end = string_position
    end

    if where_position == 0 && index_info['index_definition'][string_position, 7].upcase == ' WHERE '
      where_position = string_position + 7
    end

    string_position += 1
  end

  column_definition = index_info['index_definition']\
                      [column_definition_start + 1, column_definition_end - column_definition_start - 1]

  word_count          = 0
  parentesis_counter  = 0
  seen_space          = true
  column_list         = ''

  column_definition.each_char do |letter|
    # Here we have a second parser to ignore operator classes from index definitions
    # Operator classes are useful to say to PostgreSQL how to traverse an index
    # We just query the columns as usual

    if ![' ', ',', '(', ')'].include?(letter) && parentesis_counter == 0 && seen_space
      word_count += 1
      seen_space = false
    elsif letter == ' ' && !seen_space
      seen_space = true
    elsif letter == '('
      parentesis_counter += 1
    elsif letter == ')'
      parentesis_counter -= 1
    elsif letter == ',' && parentesis_counter == 0
      word_count = 0
      seen_space = true
    end

    column_list += letter if word_count < 2 || parentesis_counter != 0
  end

  where_clause =
    if where_position == 0
      ''
    else
      "WHERE #{index_info['index_definition'][where_position..-1]}"
    end

  print "Table: #{index_info['table_schema']}.#{index_info['table_name']} - "\
        "Index: #{index_info['index_name']} - Columns: #{column_list}, comparing data..."

  connection_object.exec('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;')
  connection_object.exec('SET enable_indexscan = off;')
  connection_object.exec('SET enable_indexonlyscan = off;')

  res_table = connection_object.exec(
    <<-SQL
      SELECT sum(('x' || substring(sum_int, 1, 8))::BIT(32)::BIGINT) result_sum
      FROM (
        SELECT md5(concat(#{column_list})) sum_int
        FROM #{index_info['table_schema']}.#{index_info['table_name']} #{where_clause}
        ORDER BY #{column_list}
      ) s;
    SQL
  )

  sum_table = res_table[0]['result_sum']
  res_table.clear

  connection_object.exec('SET enable_indexscan = on;')
  connection_object.exec('SET enable_indexonlyscan = on;')
  res_table = connection_object.exec(
    <<-SQL
      SELECT sum(('x' || substring(sum_int, 1, 8))::BIT(32)::BIGINT) result_sum
      FROM (
        SELECT md5(concat(#{column_list})) sum_int
        FROM #{index_info['table_schema']}.#{index_info['table_name']} #{where_clause}
        ORDER BY #{column_list}
      ) s;
    SQL
  )

  sum_index = res_table[0]['result_sum']
  res_table.clear

  connection_object.exec('ROLLBACK;')

  # To test an index, we force PostgreSQL to use it and then we compare
  # the results with the same query while forcing to scan the table.

  if sum_table == sum_index
    print ' index and table match - OK.'

    if index_info['indisunique'] == 't'
      print ' Index is unique or a PK, testing uniqueness...'

      where_clause = where_position == 0 ? '' : "#{index_info['index_definition'][where_position..-1]} AND"

      # The unique index test is much simpler and done directly in the database.
      res_table = connection_object.exec(
        <<-SQL
          SET enable_indexscan = off;
          SELECT count(*) as counter
          FROM (
            SELECT #{column_definition}, count(ctid)
            FROM #{index_info['table_schema']}.#{index_info['table_name']}
            WHERE #{where_clause} #{column_definition.split(',').join(' IS NOT NULL AND ') << ' IS NOT NULL'}
            GROUP BY #{column_definition}
            HAVING count(ctid) > 1
          ) as the_query;
        SQL
      )

      if res_table.getvalue(0, 0).to_i == 0
        puts ' no duplicate entries - OK.'
      else
        puts ' duplicate entries found - FAIL.'
      end

      abort if do_stop == 'y' && res_table.getvalue(0, 0).to_i == 0
    else
      puts
    end
  else
    puts " index and table don't match - FAIL."
    abort if do_stop == 'y'
  end
end

def foreign_key_check(connection_object, key_definition, do_stop)
  open_parentesis_counter = 0
  first_parentesis        = false
  string_position         = 0
  origin_start            = 0
  origin_end              = 0
  destination_start       = 0
  destination_end         = 0
  table_start             = 0
  table_end               = 0

  key_definition['constraint_definition'].each_char do |each_letter|
    # This loop is a parser to find out the origin and destination columns to test in each foreign key.
    # While it's not the most pretty parser in the world, it works as
    # expected to extract the referenced table and columns in a PostgreSQL constraint definition.

    if each_letter == '('
      open_parentesis_counter += 1
    elsif each_letter == ')'
      open_parentesis_counter -= 1
    end

    if open_parentesis_counter == 1 && !first_parentesis
      first_parentesis = true
      origin_start = string_position
    end

    if open_parentesis_counter == 0 && first_parentesis && origin_end == 0
      origin_end = string_position
    end

    if open_parentesis_counter == 1 && first_parentesis && origin_start != 0 && origin_end != 0 && \
       destination_start == 0
      destination_start = string_position
    end

    if open_parentesis_counter == 0 && first_parentesis && origin_start != 0 && origin_end != 0 && \
       destination_start != 0 && destination_end == 0
      destination_end = string_position
    end

    if table_start == 0 && key_definition['constraint_definition'][string_position, 10].upcase == 'REFERENCES'
      table_start = string_position + 11
    end

    if table_start != 0 && table_end == 0 && open_parentesis_counter == 1
      table_end = string_position
    end

    string_position += 1
  end

  origin_columns = key_definition['constraint_definition'][origin_start + 1, origin_end - origin_start - 1]
  destination_columns = key_definition['constraint_definition']\
                        [destination_start + 1, destination_end - destination_start - 1]
  destination_table = key_definition['constraint_definition'][table_start, table_end - table_start]

  print "Table: #{key_definition['table_schema']}.#{key_definition['table_name']} - "\
        "Constraint: #{key_definition['constraint_name']} - Columns: #{origin_columns} - "\
        "Referenced table: #{destination_table} - Columns: #{destination_columns}. Checking..."

  select_array = origin_columns.split(',').map { |column| "a.#{column}" }

  join_count = 0
  join_list = ''

  origin_columns.split(',').each do |column_n|
    join_list += ' AND ' unless join_count == 0

    join_list = 'a.' + column_n + ' = ' + 'b.' + destination_columns.split(',')[join_count]
    join_count += 1
  end

  # Foreign key testing is quite simple and it's done in-database too
  res_table = connection_object.exec(
    <<-SQL
      SELECT count(*) misses
      FROM (
        SELECT #{select_array.join(',')}
        FROM #{key_definition['table_schema']}.#{key_definition['table_name']} a
        LEFT JOIN #{destination_table} b ON #{join_list}
        WHERE
          #{select_array.join(' IS NOT NULL AND ')} IS NOT NULL
          AND b.id IS NULL
      ) intest;
    SQL
  )

  if res_table.getvalue(0, 0).to_i == 0
    puts ' no missing entries in referenced table - OK.'
  else
    puts ' missing entries in referenced table - FAIL.'
    abort if do_stop == 'y'
  end
end

begin
  conn_1 = PG::Connection.open(opts_db)

  puts '*** START OF INDEX/TABLE CONSISTENCY TEST ***'
  puts

  # The query will return a list of tables and their indexes to test, with additional info we need.
  query = <<-SQL
      SELECT
        pgn.nspname table_schema,
        pgct.relname table_name,
        pgci.relname index_name,
        pgi.indkey,
        pgi.indisunique,
        pga.amname index_type,
        pg_relation_size (pgct.oid::regclass) table_size,
        pg_catalog.pg_get_indexdef(pgi.indexrelid, 0, true) index_definition
      FROM
        pg_index pgi
        INNER JOIN pg_class pgct ON pgct.oid = pgi.indrelid
        INNER JOIN pg_class pgci ON pgci.oid = pgi.indexrelid
        INNER JOIN pg_namespace pgn ON pgn.oid=pgct.relnamespace
        INNER JOIN pg_am pga ON pga.oid = pgci.relam
      WHERE
        pgn.nspname !~ '^(pg_.*|information_schema)$'
        AND ( $1::text IS NULL OR pgct.oid = $1::text::regclass )
      ORDER BY table_name, index_name;
    SQL
  res_objects = conn_1.exec_params(query , [ opts_sc[:tablename] ])

  res_objects.each do |row_objects|
    if row_objects['table_size'].to_i < opts_sc[:threshold].to_i || opts_sc[:threshold].to_i == 0
      if row_objects['index_type'] == 'btree'
        index_table_match(conn_1, row_objects, opts_sc[:stop_on_fail])
      else
        puts "Table: #{row_objects['table_schema']}.#{row_objects['table_name']} - "\
             "Index: #{row_objects['index_name']}, is not btree, can't check."
      end

    else
      puts "Table: #{row_objects['table_schema']}.#{row_objects['table_name']} - "\
           "Index: #{row_objects['index_name']}, above size threshold, won't check."
    end
  end

  puts
  puts '*** END OF INDEX/TABLE CONSISTENCY TEST ***'
  puts '*** START OF FOREIGN KEYS CONSISTENCY TEST ***'
  puts

  # The query will return a list of tables and their FK constraints to test, with additional info we need.
  query = <<-SQL
      SELECT
        n.nspname as table_schema,
        c.relname as table_name,
        co.conname as constraint_name,
        pg_catalog.pg_get_constraintdef(co.oid, true) as constraint_definition,
        pg_relation_size (c.relname::regclass) table_size
      FROM
        pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_constraint co ON co.conrelid = c.oid AND co.contype = 'f'
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE
        c.relkind IN ('r','')
        AND n.nspname !~ '^(pg_.*|information_schema)$'
        AND pg_catalog.pg_table_is_visible(c.oid)
        AND ( $1::text IS NULL OR c.oid = $1::text::regclass )
      ORDER BY 1, 2;
  SQL
  res_objects = conn_1.exec(query, [ opts_sc[:tablename] ])

  res_objects.each do |row_objects|
    if row_objects['table_size'].to_i < opts_sc[:threshold].to_i || opts_sc[:threshold].to_i == 0
      foreign_key_check(conn_1, row_objects, opts_sc[:stop_on_fail])
    else
      puts "Table: #{row_objects['table_schema']}.#{row_objects['table_name']} - "\
           "Constraint: #{row_objects['constraint_name']}, above size threshold, won't check."
    end
  end
  puts '*** END OF FOREIGN KEYS CONSISTENCY TEST ***'
rescue PG::Error => e
  puts e.message
ensure
  conn_1.close if conn_1
end
