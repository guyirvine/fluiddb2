gem 'minitest'
require 'minitest/autorun'
require './lib/fluiddb2/firebird'

# FirebirdSQLTest
class FirebirdSQLTest < Minitest::Test
  def setup
    File.delete('/tmp/test.fb') if File.exist?('/tmp/test.fb')
    @db = FluidDb2::Firebird.new(URI.parse('fb://localhost/tmp/test.fb'))
    @db.execute('CREATE TABLE t1 ( f1 BIGINT, f2 VARCHAR(50) );', [])
    @db.execute('DELETE FROM t1;', [])
    @db.execute("INSERT INTO t1 ( f1, f2 ) VALUES ( 1, 'Two' );", [])
    @db.execute("INSERT INTO t1 ( f1, f2 ) VALUES ( 2, 'Three' );", [])
  end

  def test_query_for_array
    sql_in = 'SELECT f1, f2 FROM t1 WHERE f1 = 1'
    r = @db.queryForArray(sql_in, [])
    assert_equal "{\"f1\"=>1, \"f2\"=>\"Two\"}", r.to_s
  end

  def test_query_for_array_too_many_rows
    error_raised = false
    sql_in = 'SELECT f1, f2 FROM t1'

    begin
      @db.query_for_array(sql_in, [])
    rescue FluidDb::TooManyRowsError
      error_raised = true
    end

    assert_equal true, error_raised
  end

  def test_query_for_value
    sql_in = 'SELECT f2 FROM t1 WHERE f1 = 1'
    f1 = @db.queryForValue(sql_in, [])
    assert_equal 'Two', f1
  end

  def test_query_for_value_no_data_found
    error_raised = false
    sql_in = 'SELECT f1, f2 FROM t1 WHERE f1 = ?'
    begin
      @db.query_for_value(sql_in, [-1])
    rescue FluidDb::NoDataFoundError
      error_raised = true
    end
    assert_equal true, error_raised
  end

  def test_query_for_resultset
    sql_in = 'SELECT f1, f2 FROM t1 WHERE f1 > ?'
    resultset = @db.queryForResultset(sql_in, [0])
    assert_equal "[{\"f1\"=>1, \"f2\"=>\"Two\"}, {\"f1\"=>2, \"f2\"=>\"Three\"}]", resultset.to_s
  end

  def test_delete
    @db.execute('DELETE FROM t1 WHERE f1 = ?', [1])
    count = @db.queryForValue('SELECT count(*) FROM t1 WHERE f1 > ?', [0])
    assert_equal 1, count.to_i
  end

  def test_update_without_expected_affected_rows
    @db.execute('UPDATE t1 SET f2 = ? WHERE f1 = ?', ['One', 1])
    f2 = @db.query_for_value('SELECT f2 FROM t1 WHERE f1 = ?', [1])
    assert_equal 'One', f2.to_s
  end

  def test_update_with_correct_expected_affected_rows
    @db.execute('UPDATE t1 SET f2 = ? WHERE f1 = ?', ['One', 1], 1)

    f2 = @db.queryForValue('SELECT f2 FROM t1 WHERE f1 = ?', [1])
    assert_equal 'One', f2.to_s
  end

  def test_update_with_incorrect_expected_affected_rows
    error_raised = false
    begin
      @db.execute('UPDATE t1 SET f2 = ? WHERE f1 = ?', ['One', 1], 2)
    rescue FluidDb::ExpectedAffectedRowsError
      error_raised = true
    end

    f2 = @db.queryForValue('SELECT f2 FROM t1 WHERE f1 = ?', [1])
    assert_equal 'One', f2.to_s
    assert_equal true, error_raised
  end

  def test_update_with_correct_expected_matched_rows
    @db.execute('UPDATE t1 SET f2 = ? WHERE f1 = ?', ['Two', 1], 1)

    f2 = @db.queryForValue('SELECT f2 FROM t1 WHERE f1 = ?', [1])
    assert_equal 'Two', f2.to_s
  end

  def test_update_with_incorrect_expected_matched_rows
    error_raised = false
    begin
      @db.execute('UPDATE t1 SET f2 = ? WHERE f1 = ?', ['Two', 1], 2)
    rescue FluidDb::ExpectedAffectedRowsError
      error_raised = true
    end

    f2 = @db.queryForValue('SELECT f2 FROM t1 WHERE f1 = ?', [1])
    assert_equal 'Two', f2.to_s
    assert_equal true, error_raised
  end
end
