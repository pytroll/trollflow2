The launcher
------------

.. automodule:: trollflow2.launcher
    :members:
    :undoc-members:
    :show-inheritance:

It is possible to disable Posttroll Nameserver usage for the incoming
messages by starting ``satpy_launcher.py`` with command-line arguments
``-n False -a tcp://<host>:<port>`` where the host and port point to a
message publisher. Multiple publisher addresses can be given by supplying
them with additional ``-a`` switches.

The Trollflow2 process can be terminated gracefully by sending SIGTERM
to the it, eg. by ``kill -SIGTERM <pid>`` where ``<pid>`` is the
process ID number of Trollflow2.  After this no new messages will be
accepted, and the completion of currently running processing will exit
Trollflow2.
