Weir
====
Weir is a Python API for ZFS.

The API is loosely based on libzfs, but is intended to be more Pythonic
rather than reproducing the C API exactly.  Key differences include:

- ZFS datasets are represented as objects.

- ``send()`` and ``receive()`` can return an open file.

Weir is implemented on top of the command-line ``zfs`` and ``zpool``
commands for portability and so as to facilitate support for remote
operation without requiring installation on the remote host. Support
for remote operation will be the focus for Weir 0.3.0.

Installation
------------
Requires Python 2.6 or 2.7.

To install Weir, simply:

``$ pip install weir``

Usage
-----
eg to find a filesystem's most recent snapshot:

	>>> from weir import zfs
	>>> zfs.open('zroot/test').snapshots()[-1].name
	'zroot/test@auto-2015-02-02T112547'

License
-------
Licensed under the Common Development and Distribution License (CDDL).
