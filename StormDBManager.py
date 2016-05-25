from storm.database import Connection
from storm.locals import *
from storm.twisted.transact import Transactor, transact
from twisted.internet.defer import DeferredLock

from twisted.internet import reactor


class StormDBManager:
    """
    The StormDBManager is a manager that runs queries using the Storm Framework.
    These queries will be run on the Twisted thread-pool to ensure asynchronous, non-blocking behavior.
    In the future, this database manager will be the basis to an ORM based approach.
    """

    def __init__(self, db_path):
        """
        Sets up the database and all necessary elements for the
        database manager to function.
        """
        # This is currently an IN MEMORY database
        self._database = create_database(db_path)

        # The transactor is required when you have methods decorated with the @transact decorator
        # This field name must NOT be changed.
        self.transactor = Transactor(reactor.getThreadPool())

        # Create a DeferredLock that should be used by callers to schedule their call.
        self.db_lock = DeferredLock()

    def schedule_query(*args, **kwargs):
        """
        Utility function to schedule a query to be executed using the db_lock.
        :param args: The arguments of which the first is self and the second the function to be run.
        Any additional arguments will be passed as the function arguments.
        :param kwargs: Keyword arguments that are passed to the function
        :return: A deferred that fires with the result of the query.
        """
        if len(args) < 2:
            if not args:
                raise TypeError("run() takes at least 2 arguments, none given.")
            raise TypeError("%s.run() takes at least 2 arguments, 1 given" % (
                args[0].__class__.__name__,))
        self, f = args[:2]
        args = args[2:]

        return self.db_lock.run(f, *args, **kwargs)

    @transact
    def execute_query(self, query, arguments=None):
        """
        Executes a query on the twisted thread-pool using the storm framework.
        :param query: The sql query to be executed
        :param arguments: Optional arguments that go with the sql query
        :return: None as this is function is executed on the thread-pool, database objects
        such as cursors cannot be returned.
        """
        connection = Connection(self._database)
        connection.execute(query, arguments, noresult=True)
        connection.commit()
        connection.close()

    @transact
    def fetch_one(self, query, arguments=None):
        """
        Executes a query on the twisted thread-pool using the storm framework and returns the first result.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with the first tuple that matches the query or None.
        The result would be the same as using execute and calling the next() function on it.
        """
        connection = Connection(self._database)
        result = connection.execute(query, arguments).get_one()
        connection.commit()
        connection.close()
        return result

    @transact
    def fetch_all(self, query, arguments=None):
        """
        Executes a query on the twisted thread-pool using the storm framework and
        returns all results as a list of tuples.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with a list of tuple results that matches the query or an empty list.
        """
        connection = Connection(self._database)
        return connection.execute(query, arguments).get_all()

    @transact
    def insert(self, table_name, **argv):
        """
        Inserts data provided as keyword arguments into the table provided as an argument.
        :param table_name: The name of the table the data has to be inserted into.
        :param argv: A dictionary where the key represents the column and the value the value to be inserted.
        :return: A deferred that fires when the data has been inserted.
        """
        connection = Connection(self._database)
        self._insert(connection, table_name, **argv)
        connection.commit()
        connection.close()

    def _insert(self, connection, table_name, **argv):
        """
        Utility function to insert data which is not decorated by the @transact to prevent
        a loop calling this function to create many threads.
        Do NOT call this function on the main thread, it will be then blocking on that thread.
        :param connection: The database connection object.
        :param table_name: The name of the table the data has to be inserted into.
        :param argv: A dictionary where the key represents the column and the value the value to be inserted.
        :return: A deferred that fires when the data has been inserted.
        """
        if len(argv) == 0: return
        if len(argv) == 1:
            sql = u'INSERT INTO %s (%s) VALUES (?);' % (table_name, argv.keys()[0])
        else:
            questions = '?,' * len(argv)
            sql = u'INSERT INTO %s %s VALUES (%s);' % (table_name, tuple(argv.keys()), questions[:-1])

        connection.execute(sql, argv.values(), noresult=True)

    @transact
    def insert_many(self, table_name, arg_list):
        """
        Inserts many items into the database
        :param table_name: the table name that you want to insert to.
        :param arg_list: A list contaning dictionaries where the key is the column name and
        the value the value to be inserted into this column.
        :return: A deferred that fires once the bulk insertion is done.
        """
        if len(arg_list) == 0: return
        connection = Connection(self._database)
        for args in arg_list:
            self._insert(connection, table_name, **args)

        connection.commit()
        connection.close()

    def delete(self, table_name, **argv):
        """
        Utility function to delete from the database.
        :param table_name: the table name to delete from
        :param argv: A dictionary containing key values.
        The key is the column to target and the value can be a tuple or single element.
        In case the value is a tuple, it can specify the operator.
        In case the value is a single element, the equals "=" operator is used.
        :return: A deferred that fires when the deletion has been performed.
        """
        sql = u'DELETE FROM %s WHERE ' % table_name
        arg = []
        for k, v in argv.iteritems():
            if isinstance(v, tuple):
                sql += u'%s %s ? AND ' % (k, v[0])
                arg.append(v[1])
            else:
                sql += u'%s=? AND ' % k
                arg.append(v)
        sql = sql[:-5] # Remove the last AND
        return self.execute_query(sql, arg)

    def num_rows(self, table_name):
        """
        Utility function to get the number of rows of a table.
        :param table_name: The table name
        :return: A deferred that fires with the number of rows in the table.
        """
        sql = u"SELECT count(*) FROM %s LIMIT 1" % table_name
        result = self.fetch_one(sql)
        return result
