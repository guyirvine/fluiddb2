require 'minitest/autorun'
require './lib/fluiddb2/mysql'

# MysqlSQLTest
class MysqlSQLTest < Minitest::Test
  def setup
    cs = 'mysql://vagrant:password@localhost/test'
    @db = FluidDb2::Mysql.new(URI.parse(cs))
    @db.execute('DROP TABLE IF EXISTS table1')
    @db.execute('CREATE TABLE table1 ( id BIGINT NOT NULL AUTO_INCREMENT, ' \
                'field1 BIGINT, field2 VARCHAR(50), PRIMARY KEY (id) );')

    @db.execute("INSERT INTO table1 ( field1, field2 ) VALUES ( 1, 'Two' );")
    @db.execute("INSERT INTO table1 ( field1, field2 ) VALUES ( 2, 'Three' );")
  end

  def test_query_for_array
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 = 1'
    r = @db.query_for_array(sql_in)
    assert_equal "{\"field1\"=>\"1\", \"field2\"=>\"Two\"}", r.to_s
  end

  def test_query_for_array_too_many_rows
    error_raised = false
    sql_in = 'SELECT field1, field2 FROM table1'

    begin
      @db.query_for_array(sql_in)
    rescue FluidDb2::TooManyRowsError
      error_raised = true
    end

    assert_equal true, error_raised
  end

  def test_query_for_value
    sql_in = 'SELECT field2 FROM table1 WHERE field1 = 1'
    field1 = @db.query_for_value(sql_in)

    assert_equal 'Two', field1
  end

  def test_query_for_value_no_data_found
    error_raised = false
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 = ?'

    begin
      @db.query_for_value(sql_in, [-1])
    rescue FluidDb2::NoDataFoundError
      error_raised = true
    end

    assert_equal true, error_raised
  end

  def test_query_for_resultset
    sql_in = 'SELECT field1, field2 FROM table1 WHERE field1 > ?'
    resultset = @db.query_for_resultset(sql_in, [0])

    assert_equal "[{\"field1\"=>\"1\", \"field2\"=>\"Two\"}, {\"field1\"=>\"2\", \"field2\"=>\"Three\"}]", resultset.to_s
  end

  def test_delete
    @db.execute('DELETE FROM table1 WHERE field1 = ?', [1])
    count = @db.query_for_value('SELECT count(*) FROM table1 WHERE field1 > ?',
                              [0])
    assert_equal 1, count.to_i
  end

  def test_update_without_expected_affected_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?', ['One', 1])
    field2 = @db.query_for_value('SELECT field2 FROM table1 WHERE field1 = ?',
                                 [1])
    assert_equal 'One', field2.to_s
  end

  def test_update_with_correct_expected_affected_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?',
                ['One', 1],
                1)
    field2 = @db.query_for_value('SELECT field2 FROM table1 WHERE field1 = ?',
                               [1])
    assert_equal 'One', field2.to_s
  end

  def test_update_with_incorrect_expected_affected_rows
    error_raised = false
    begin
      @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?',
                  ['One', 1],
                  2)
    rescue FluidDb2::ExpectedAffectedRowsError
      error_raised = true
    end

    field2 = @db.query_for_value('SELECT field2 FROM table1 WHERE field1 = ?',
                               [1])
    assert_equal 'One', field2.to_s
    assert_equal true, error_raised
  end

  def test_update_with_correct_expected_matched_rows
    @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?', ['Two', 1], 1)
    field2 = @db.query_for_value('SELECT field2 FROM table1 WHERE field1 = ?',
                               [1])
    assert_equal 'Two', field2.to_s
  end

  def test_update_with_incorrect_expected_matched_rows
    error_raised = false
    begin
      @db.execute('UPDATE table1 SET field2 = ? WHERE field1 = ?',
                  ['Two', 1],
                  2)
    rescue FluidDb2::ExpectedAffectedRowsError
      error_raised = true
    end

    field2 = @db.query_for_value('SELECT field2 FROM table1 WHERE field1 = ?',
                               [1])
    assert_equal 'Two', field2.to_s
    assert_equal true, error_raised
  end

  def test_insert
    @db.insert('INSERT INTO table1 ( field1, field2 ) VALUES ( ?, ? );',
               [3, 'Four'])
  end
end
