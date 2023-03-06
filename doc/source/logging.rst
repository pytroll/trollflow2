Log configuration
-----------------

By default Trollflow2 will log everything in all the used packages and modules from DEBUG level and above
to the console. The ``sapty_launcher.py`` has the possibility for user-defined log configuration via the `-c`
option. The log configuration is given in a YAML file. See below for examples to build up from.

Log verbosity
+++++++++++++

Some of the used libraries are very verbose, so the real information can be hidden amongst the hunderds of
DEBUG messages. The first example logs only INFO messages from Trollflow2 and its own modules, and only
WARNING messages from all the other packages and possible external user-created modules.

.. code-block:: yaml

  version: 1
  formatters:
    fmt:
      format: '[%(asctime)s %(levelname)-8s %(name)s] %(message)s'
  handlers:
    console_handler:
      class: logging.StreamHandler
      formatter: fmt
  loggers:
    '':
      level: WARNING
      handlers: [console_handler]
    'trollflow2':
      level: INFO

The logger named ``''`` is the root logger, and its settings will be used by all loggers not defined in the
configuration file.

Finer control for different loggers (packages and their modules) can be controlled by adding a new section under
``loggers``. For example getting DEBUG level (and above) messages from Pyspectral, this can be used:

.. code-block:: yaml

    'pyspectral':
      level: DEBUG


Logging to a file
+++++++++++++++++

If Trollflow2 is run independently (not via Supervisord, Kubernetes, or such system that can handle the log messges)
it is useful to save the log messages to files.

The below example saves the messages to a file, that is rotated at midnight and the completed log file is moved to
a new filename having the date appended to it. The number of days the log files are kept can be controlled via the
``backupCount`` argument of the ``file_handler`` section. The example also sets the time stamps to UTC, which is
handy with satellite data that is also in UTC time.

.. code-block:: yaml

  version: 1
  formatters:
    fmt:
      format: '[%(asctime)s %(levelname)-8s %(name)s] %(message)s'
  handlers:
    file_handler:
      class: logging.TimedRotatingFileHandler
      formatter: fmt
      when: midnight
      backupCount: 14
      utc: true
  loggers:
    '':
      level: WARNING
      handlers: [console]
    'trollflow2':
      level: INFO
