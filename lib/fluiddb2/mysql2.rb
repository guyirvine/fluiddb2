require 'fluiddb2'
require 'mysql2'

module FluidDb2
  # Mysql2
  class Mysql2 < Base
    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def connect
      uri = @uri
      @connection = ::Mysql2::Client.new(:host => uri.host,
                                         :database => uri.path.sub('/', ''),
                                         :username => uri.user,
                                         :password => uri.password,
                                         :flags => ::Mysql2::Client::FOUND_ROWS)
    end

    def close
      @connection.close
    end

    def query_for_array(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.count
      when -1
        fail FluidDb2::ConnectionError
      when 0
        fail FluidDb2::NoDataFoundError
      when 1
        r = nil
        results.each do |row|
          r = row
        end
        return r
      else
        fail FluidDb2::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.query(sql, :as => :array)

      case results.count
      when -1
        fail FluidDb2::ConnectionError
      when 0
        fail FluidDb2::NoDataFoundError
      when 1
        r = nil
        results.each do |row|
          r = row
        end
        return r[0]
      else
        fail FluidDb2::TooManyRowsError
      end
    end

    def query_for_resultset(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.count
      when -1
        fail FluidDb2::ConnectionError
      else
        list = []
        results.each do |row|
          list.push row
        end
        return list
      end
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = FluidDb2.format_to_sql(sql, params)
      @connection.query(sql)

      if !expected_affected_rows.nil? && @connection.affected_rows != expected_affected_rows
        raise ExpectedAffectedRowsError.new( "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{@connection.affected_rows}")
      end
    end

    def insert(sql, params)
      execute(sql, params)
      @connection.last_id
    end
  end
end
