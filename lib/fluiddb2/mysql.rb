require 'FluidDb2'
require 'mysql'

module FluidDb2
  # Mysql
  class Mysql < Base
    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def connect
      uri = @uri
      database = uri.path.sub('/', '')

      @connection = ::Mysql.new uri.host, uri.user, uri.password, database, nil, nil, ::Mysql::CLIENT_FOUND_ROWS
    end

    def close
      @connection.close
    end

    def query_for_array(sql, params = [])
      sql = format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.num_rows
      when -1
        fail FluidDb::ConnectionError
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        r = results.fetch_hash
        return r
      else
        fail FluidDb::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.num_rows
      when -1
        fail FluidDb::ConnectionError
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        r = nil
        results.each do |row|
          r = row
        end
        return r[0]
      else
        fail FluidDb::TooManyRowsError
      end
    end

    def query_for_resultset(sql, params = [])
      sql = format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.num_rows
      when -1
        fail FluidDb::ConnectionError
      else
        list = []
        results.each_hash do |row|
          list.push row
        end
        return list
      end
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = format_to_sql(sql, params)
      @connection.query(sql)

      if !expected_affected_rows.nil? && @connection.affected_rows != expected_affected_rows
        fail ExpectedAffectedRowsError, "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{@connection.affected_rows}"
      end
    end

    def insert(sql, params)
      execute(sql, params)
      @connection.insert_id
    end
  end
end
