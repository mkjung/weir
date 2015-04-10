Weir
====
Weir is a Python API for ZFS.

The API is loosely based on libzfs, but is intended to be more Pythonic
rather than reproducing the C API exactly.  Key differences include:

- ZFS datasets are represented as objects.

- ``send()`` and ``receive()`` can return an open file.

Weir is implemented on top of the command-line ``zfs`` and ``zpool``
commands for portability and so as to facilitate support for remote
operation without requiring installation on the remote host.

Remote datasets can be specified with urls of the form
``zfs://user@host/path@snapname``.

Installation
------------
Requires Python 2.6, 2.7 or 3.4+.

To install Weir, simply:

``$ pip install weir``

Usage
-----
eg to find a filesystem's most recent snapshot:

	>>> from weir import zfs
	>>> zfs.open('zfs://backup.local/wanaka/test').snapshots()[-1].name
	'zfs://backup.local/wanaka/test@auto-2015-04-08T145240'

License
-------
Licensed under the Common Development and Distribution License (CDDL).
