import logging
from storm.database import Connection
from storm.exceptions import OperationalError
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
        self._logger = logging.getLogger(self.__class__.__name__)

        self.db_path = db_path

        # The transactor is required when you have methods decorated with the @transact decorator
        # This field name must NOT be changed.
        self.transactor = Transactor(reactor.getThreadPool())

        # Create a DeferredLock that should be used by callers to schedule their call.
        self.db_lock = DeferredLock()

    def initialize(self):
        """
        Opens/creates the database and initializes the version.
        """
        # Open or create the database
        self._database = create_database(self.db_path)
        self.connection = Connection(self._database)
        self._version = 0
        self._retrieve_version()

    @property
    def version(self):
        return self._version

    def _retrieve_version(self):
        """
        Attempts to retrieve the current datbase version from the MyInfo table.
        If it fails, the _version field remains at 1 as defined in the init function.
        """
        def on_result(result):
            if result:
                version_str = result[0]
                self._version = int(version_str)
                self._logger.info(u"Current database version is %s", self._version)

        def on_error(_):
            self._logger.warning(u"Failed to load database version, setting the DB vesion to 0.")

        # Schedule the query and add a callback and errback to the deferred.
        # return self.fetchone(u"SELECT value FROM MyInfo WHERE entry == 'version'").addCallbacks(on_result, on_error)

        # TEMPORARILY TRY CATCH BECAUSE WE WANT A SYNCHRONOUS, NON-DEFERRED INTERFACE FOR NOW.
        try:
            result = self.fetchone(u"SELECT value FROM MyInfo WHERE entry == 'version'")
            version_str = result[0]
            self._version = int(version_str)
            self._logger.info(u"Current database version is %s", self._version)

        except OperationalError:
            self._version = 0

    def schedule_query(self, callable, *args, **kwargs):
        """
        Utility function to schedule a query to be executed using the db_lock.
        :param callable: The database function that is to be executed.
        :param args: Any additional arguments that will be passed as the callable's arguments.
        :param kwargs: Keyword arguments that are passed to the callable function.
        :return: A deferred that fires with the result of the query.
        """

        return self.db_lock.run(callable, *args, **kwargs)

    def execute(self, query, arguments=None, get_lastrowid=False):
        """
        Executes a query on the twisted thread-pool using the storm framework.
        :param query: The sql query to be executed
        :param arguments: Optional arguments that go with the sql query
        :return: A deferred that fires once the execution is done, the result will be None.
        """

        # @transact
        def _execute(self, query, arguments=None, get_lastrowid=False):
            # connection = Connection(self._database)
            ret = None
            if get_lastrowid:
                result = self.connection.execute(query, arguments, noresult=False)
                ret = result._raw_cursor.lastrowid
            else:
                self.connection.execute(query, arguments, noresult=True)
            # connection.close()
            return ret

        return _execute(self, query, arguments, get_lastrowid)
        #return self.db_lock.run(_execute, self, query, arguments, get_lastrowid)

    def executemany(self, query, list):
        """
            Executes a query on the twisted thread-pool using the storm framework many times using the
            values provided by a list.
            :param query: The sql query to be executed
            :param list: The list containing tuples of values to execute the query with.
            :return: A deferred that fires once the execution is done, the result will be None.
            """

        for item in list:
            self.execute(query, item)

    def executescript(self, sql_statements):
        """
        Executes a script of several sql queries sequentially.
        Note that this function does exist in SQLite, but not in the storm framework:
        https://www.mail-archive.com/storm@lists.canonical.com/msg00569.html
        Args:
            sql_statements: A list of sql statements to be executed.

        Returns: None

        """
        for sql_statement in sql_statements:
            self.connection.execute(sql_statement)

    def fetchone(self, query, arguments=None):
        """
        Executes a query on the twisted thread-pool using the storm framework and returns the first result.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with the first tuple that matches the query or None.
        The result would be the same as using execute and calling the next() function on it.
        """

        # @transact
        def _fetchone(self, query, arguments=None):
            # connection = Connection(self._database)
            result = self.connection.execute(query, arguments).get_one()
            # connection.close()
            return result

        return _fetchone(self, query, arguments)
        # return self.db_lock.run(_fetchone, self, query, arguments)

    def fetchall(self, query, arguments=None):
        """
        Executes a query on the twisted thread-pool using the storm framework and
        returns all results as a list of tuples.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with a list of tuple results that matches the query or an empty list.
        """

        # @transact
        def _fetchall(self, query, arguments=None):
            # connection = Connection(self._database)
            return self.connection.execute(query, arguments).get_all()

        # return self.db_lock.run(_fetchall, self, query, arguments)
        return _fetchall(self, query, arguments)

    def insert(self, table_name, **kwargs):
        """
        Inserts data provided as keyword arguments into the table provided as an argument.
        :param table_name: The name of the table the data has to be inserted into.
        :param argv: A dictionary where the key represents the column and the value the value to be inserted.
        :return: A deferred that fires when the data has been inserted.
        """
        return self._insert(table_name, **kwargs)
        # return self.db_lock.run(self._insert, table_name, **kwargs)

    # @transact
    def _insert(self, table_name, **kwargs):
        # connection = Connection(self._database)
        self.__insert(self.connection, table_name, **kwargs)
        # connection.close()

    def __insert(self, connection, table_name, **kwargs):
        """
        Utility function to insert data which is not decorated by the @transact to prevent
        a loop calling this function to create many threads.
        Do NOT call this function on the main thread, it will be then blocking on that thread.
        :param connection: The database connection object.
        :param table_name: The name of the table the data has to be inserted into.
        :param argv: A dictionary where the key represents the column and the value the value to be inserted.
        :return: A deferred that fires when the data has been inserted.
        """
        if len(kwargs) == 0: raise ValueError("No keyword arguments supplied.")
        if len(kwargs) == 1:
            sql = u'INSERT INTO %s (%s) VALUES (?);' % (table_name, kwargs.keys()[0])
        else:
            questions = ','.join(('?',)*len(kwargs))
            sql = u'INSERT INTO %s %s VALUES (%s);' % (table_name, tuple(kwargs.keys()), questions)

            self.connection.execute(sql, kwargs.values(), noresult=True)

    def insert_many(self, table_name, arg_list):
        """
        Inserts many items into the database
        :param table_name: the table name that you want to insert to.
        :param arg_list: A list contaning dictionaries where the key is the column name and
        the value the value to be inserted into this column.
        :return: A deferred that fires once the bulk insertion is done.
        """

        # @transact
        def _insertmany(self, table_name, arg_list):
            if len(arg_list) == 0: return
            # connection = Connection(self._database)
            for args in arg_list:
                self.__insert(self.connection, table_name, **args)
            # connection.close()

        return _insertmany(self, table_name, arg_list)
        # return self.db_lock.run(_insertmany, self, table_name, arg_list)

    def delete(self, table_name, **kwargs):
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
        for k, v in kwargs.iteritems():
            if isinstance(v, tuple):
                sql += u'%s %s ? AND ' % (k, v[0])
                arg.append(v[1])
            else:
                sql += u'%s=? AND ' % k
                arg.append(v)
        sql = sql[:-5] # Remove the last AND
        return self.execute(sql, arg)

    def count(self, table_name):
        """
        Utility function to get the number of rows of a table.
        :param table_name: The table name
        :return: A deferred that fires with the number of rows in the table.
        """
        sql = u"SELECT count(*) FROM %s LIMIT 1" % table_name
        result = self.fetchone(sql)
        return result
