require 'FluidDb2'
require 'fb'
include Fb

module FluidDb2
  # Firebird
  class Firebird < Base
    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def connect
      uri = @uri

      user = uri.user || 'sysdba'
      password = uri.password || 'masterkey'
      port = uri.port || 3050

      path = uri.path
      path = path.slice(1, uri.path.length - 1) if path.slice(0, 3) == '/C:'
      path = URI.unescape(path)

      # The Database class acts as a factory for Connections.
      # It can also create and drop databases.
      db = Database.new(:database => "#{uri.host}/#{port}:#{path}",
                        :username => user,
                        :password => password)
      # :database is the only parameter without a default.
      # Let's connect to the database, creating it if it doesn't already exist.
      @connection = db.connect rescue db.create.connect
    end

    def close
      @connection.close
    end

    def query_for_array(sql, params = [])
      sql = format_to_sql(sql, params)
      list = @connection.query(:hash, sql)

      case list.length
      when -1
        fail FluidDb2::ConnectionError
      when 0
        fail FluidDb2::NoDataFoundError
      when 1
        return list[0]
      else
        fail FluidDb2::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = format_to_sql(sql, params)
      results = @connection.query(sql)

      case results.length
      when -1
        fail FluidDb::ConnectionError
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        return results[0][0]
      else
        fail FluidDb::TooManyRowsError
      end
    end

    def query_for_resultset(sql, params = [])
      sql = format_to_sql(sql, params)
      list = @connection.query(:hash, sql)

      case list.length
      when -1
        fail FluidDb::ConnectionError
      else
        return list
      end
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = format_to_sql(sql, params)
      verbose_log "#{self.class.name}.execute. #{sql}"
      affected_rows = @connection.execute(sql)

      if !expected_affected_rows.nil? && affected_rows != expected_affected_rows
        fail ExpectedAffectedRowsError,
              "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{affected_rows}"
      end
    end

    def exec_params(sql, params, expected_affected_rows = nil)
      parts = sql.split('?')
      sql = ''
      parts.each_with_index do |p, idx|
        sql += p
        sql += "$#{idx + 1}" if idx < parts.length - 1
      end
      affected_rows = @connection.exec_params(sql, params)

      if !expected_affected_rows.nil? && affected_rows != expected_affected_rows
        fail ExpectedAffectedRowsError,
             "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{affected_rows}"
      end
    end

    def insert(_sql, _params)
      fail 'Firebird uses SEQUENCES, so possibly easier to use 2 executes'
    end

    # Transaction Semantics
    def begin
      @connection.transaction
    end

    # Transaction Semantics
    def commit
      @connection.commit
    end

    # Transaction Semantics
    def rollback
      @connection.rollback
    end
  end
end
