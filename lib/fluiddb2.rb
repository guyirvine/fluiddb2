require 'date'
require 'time'
require 'uri'

# FluidDb2
module FluidDb2
  class ConnectionError < StandardError
  end
  class NoDataFoundError < StandardError
  end
  class TooManyRowsError < StandardError
  end
  class ParamTypeNotSupportedError < StandardError
  end
  class ExpectedAffectedRowsError < StandardError
  end
  class IncorrectNumberOfParametersError < StandardError
  end
  class DuplicateKeyError < StandardError
  end

  def self.db(uri)
    uri = URI.parse(uri) if uri.is_a? String

    case uri.scheme
    when 'mysql'
      require 'fluiddb2/mysql'
      return FluidDb2::Mysql.new(uri)
    when 'mysql2'
      require 'fluiddb2/mysql2'
      return FluidDb2::Mysql2.new(uri)
    when 'pgsql'
      require 'fluiddb2/pgsql'
      return FluidDb2::Pgsql.new(uri)
    when 'fb'
      require 'fluiddb2/firebird'
      return FluidDb2::Firebird.new(uri)
    when 'mock'
      require 'fluiddb2/Mock'
      return FluidDb2::Mock.new(uri)
    when 'tinytds'
      require 'fluiddb2/tiny_tds'
      return FluidDb2::TinyTds.new(uri)
    when 'sqlite'
      require 'fluiddb2/sqlite'
      return FluidDb2::SQLite.new(uri)
    else
      abort("Scheme, #{uri.scheme}, not recognised when configuring creating " \
             'db connection')
    end
  end

  def self.splice_sql(sql, params)
    fail IncorrectNumberOfParametersError if params.length != sql.count('?')

    sql_out = ''
    sql.split('?').each_with_index do |s, idx|
      sql_out += s
      sql_out += params[idx] unless params[idx].nil?
    end

    sql_out
  end

  def self.escape_string(input)
    input.split("'").join("''")
  end

  def self.format_to_sql(sql, params = nil)
    return sql if params.nil? || params.count == 0

    params.each_with_index do |v, idx|
      if v.is_a? String
        v = "'#{escape_string(v)}'"
      elsif v.is_a? DateTime
        v = "'" + v.strftime('%Y-%m-%d %H:%M:%S.%6N %z') + "'"
      elsif v.is_a? Time
        v = "'" + v.strftime('%Y-%m-%d %H:%M:%S.%6N %z') + "'"
      elsif v.is_a? Date
        v = "'#{v}'"
      elsif v.is_a? Numeric
        v = v.to_s
      elsif v.is_a? TrueClass
        v = 'true'
      elsif v.is_a? FalseClass
        v = 'false'
      elsif v.nil?
        v = 'NULL'
      else
        fail ParamTypeNotSupportedError,
             "Name of unknown param type, #{v.class.name}, for sql, #{sql}"
      end
      params[idx] = v
    end

    sql_out = splice_sql(sql, params)
    if @verbose == true
      puts self.class.name
      puts sql
      puts params.join(',')
      puts sql_out
    end

    sql_out
  end

  def self.convert_tuple_to_hash(fields, tuple, j)
    hash = {}
    0.upto(fields.length - 1).each do |i|
      hash[fields[i].to_s] = tuple.getvalue(j, i)
    end

    hash
  end

  # Base
  class Base
    attr_writer :verbose
    attr_reader :connection

    # Constructor.
    #
    # @param [String] uri a location for the resource to which we will attach,
    #  eg mysql://user:pass@127.0.0.1/foo
    def initialize(uri)
      if uri.is_a? String
        @uri = URI.parse(uri)
      else
        @uri = uri
      end

      connect

      @verbose = !ENV['VERBOSE'].nil?
    end

    def verbose_log(string)
      puts string if @verbose == true
    end

    def connect
      fail NotImplementedError, "You must implement 'connect'."
    end

    def close
      fail NotImplementedError, "You must implement 'close'."
    end

    def reconnect
      close
      connect
    end

    # Return a single row from the database, given the sql parameter.
    # Throws an error for no data.
    # Throws an error for more than 1 row
    #
    # @param [String] sql The SELECT statement to run
    # @param [Array] parama The parameters to be added to the sql query. Ruby
    #  types are used to determine formatting and escaping.
    def query_for_array(_sql, _params)
      fail NotImplementedError, "You must implement 'queryForArray'."
    end

    # Return a single value is returned from a single row from the database,
    #  given the sql parameter.
    # Throws an error for no data.
    # Throws an error for more than 1 row
    #
    # @param [String] sql The SELECT statement to run
    # @param [Array] parama The parameters to be added to the sql query. Ruby
    #  types are used to determine formatting and escaping.
    def query_for_value(_sql, _params)
      fail NotImplementedError, "You must implement 'queryForValue'."
    end

    def query_for_resultset(_sql, _params)
      fail NotImplementedError, "You must implement 'queryForResultset'."
    end

    # Execute an insert, update or delete, then check the impact that statement
    #  has on the data.
    #
    # @param [String] sql The SELECT statement to run
    # @param [Array] parama The parameters to be added to the sql query. Ruby
    #  types are used to determine formatting and escaping.
    # @param [String] expected_affected_rows The number of rows that should
    #  have been updated.
    def execute(_sql, _params, _expected_affected_rows)
      fail NotImplementedError, "You must implement 'execute'."
    end

    # Transaction Semantics
    def begin
      @connection.execute('BEGIN', [])
    end

    # Transaction Semantics
    def commit
      @connection.execute('COMMIT', [])
    end

    # Transaction Semantics
    def rollback
      @connection.execute('ROLLBACK', [])
    end
  end
end
