require 'fluiddb2'
require 'pg'

module FluidDb2
  # Pgsql
  class Pgsql < Base
    # Connect to Db.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def connect
      uri = @uri
      host = uri.host
      dbname = uri.path.sub('/', '')

      hash = Hash['host', host, 'dbname', dbname]
      hash['port'] = uri.port unless uri.port.nil?
      hash['user'] = uri.user unless uri.user.nil?
      hash['password'] = uri.password unless uri.password.nil?

      @connection = PG.connect(hash)
    end

    def close
      @connection.close
    end

    def query_for_array(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.exec(sql)

      case results.num_tuples
      when -1
        fail FluidDb2::ConnectionError
      when 0
        fail FluidDb2::NoDataFoundError
      when 1
        return FluidDb2.convert_tuple_to_hash(results.fields, results, 0)
      else
        fail FluidDb2::TooManyRowsError
      end
    end

    def query_for_value(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.exec(sql)

      case results.num_tuples
      when -1
        fail FluidDb2::ConnectionError
      when 0
        fail FluidDb2::NoDataFoundError
      when 1
        return results.getvalue(0, 0)
      else
        fail FluidDb2::TooManyRowsError
      end
    end

    def query_for_resultset(sql, params = [])
      sql = FluidDb2.format_to_sql(sql, params)
      results = @connection.exec(sql)

      case results.num_tuples
      when -1
        fail FluidDb2::ConnectionError
      else
        list = []
        fields = results.fields
        0.upto(results.ntuples - 1) do |nbr|
          list.push FluidDb2.convert_tuple_to_hash(fields, results, nbr)
        end

        return list
      end
    end

    def execute(sql, params = [], expected_affected_rows = nil)
      sql = FluidDb2.format_to_sql(sql, params)

      verbose_log "#{self.class.name}.execute. #{sql}"
      r = @connection.exec(sql)

      if !expected_affected_rows.nil? && r.cmd_tuples != expected_affected_rows
        fail ExpectedAffectedRowsError, "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}"
      end
    rescue PG::Error => e
      raise DuplicateKeyError(e.message) unless e.message.index('duplicate key value violates unique constraint').nil?
      raise e
    end

    def exec_params(sql, params = [], expected_affected_rows = nil)
      parts = sql.split('?')
      sql = ''
      parts.each_with_index do |p, idx|
        sql += p
        sql += "$#{idx + 1}" if idx < parts.length - 1
      end
      r = @connection.exec_params(sql, params)

      if !expected_affected_rows.nil? and r.cmd_tuples != expected_affected_rows
        fail ExpectedAffectedRowsError, "Expected affected rows, #{expected_affected_rows}, Actual affected rows, #{r.cmd_tuples}"
      end
    rescue PG::Error => e
      raise DuplicateKeyError(e.message) unless e.message.index("duplicate key value violates unique constraint").nil?
      raise e
    end

    # Transaction Semantics
    def begin
      @connection.exec('BEGIN', [])
    end

    # Transaction Semantics
    def commit
      @connection.exec('COMMIT', [])
    end

    # Transaction Semantics
    def rollback
      @connection.exec('ROLLBACK', [])
    end

    def insert(_sql, _params)
      fail 'Pgsql uses SEQUENCES, so possibly easier to use 2 executes'
    end
  end
end
