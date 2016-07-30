require 'FluidDb2'
require 'sqlite3'

module FluidDb2
  # SQLite
  class SQLite < Base
    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def connect
      uri = @uri
      @connection = SQLite3::Database.new uri.path
    end

    def close
      @connection.close
    end

    def query_for_array(sql, params = [])
      sql = format_to_sql(sql, params)
      @connection.results_as_hash = true
      results = @connection.execute(sql)

      case results.length
      when -1
        fail FluidDb::ConnectionError
      when 0
        fail FluidDb::NoDataFoundError
      when 1
        return results[0]
      else
        fail FluidDb::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = format_to_sql(sql, params)
      @connection.results_as_hash = false
      results = @connection.execute(sql)

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
      @connection.results_as_hash = true
      results = @connection.execute(sql)

      case results.length
      when -1
        fail FluidDb::ConnectionError
      else
        return results
      end
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = format_to_sql(sql, params)

      verbose_log "#{self.class.name}.execute. #{sql}"
      r = @connection.execute(sql)

      if !expected_affected_rows.nil? && r.changes != expected_affected_rows
        fail ExpectedAffectedRowsError, "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}"
      end
    end

    def exec_params(sql, params = [], expected_affected_rows = nil)
      parts = sql.split('?')
      sql = ''
      parts.each_with_index do |p, idx|
        sql += p
        sql += "$#{idx + 1}" if idx < parts.length - 1
      end
      r = @connection.exec_params(sql, params)

      if !expected_affected_rows.nil? && r.changes != expected_affected_rows
        fail ExpectedAffectedRowsError, "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}"
      end
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

    def insert(_sql, _params)
      fail 'SQLite uses SEQUENCES, so possibly easier to use 2 executes'
    end
  end
end
