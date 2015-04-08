import logging
try:
	from urllib.parse import urlsplit
except ImportError:
	from urlparse import urlsplit

from weir import process

log = logging.getLogger(__name__)

# Split a dataset url into netloc and local dataset parts
def _split_dataset(url):
	parts = urlsplit(url)
	return parts.netloc, parts.path.strip('/')

# Internal factory function to instantiate dataset object
def _dataset(type, name):
	if type == 'volume':
		return ZFSVolume(name)

	if type == 'filesystem':
		return ZFSFilesystem(name)

	if type == 'snapshot':
		return ZFSSnapshot(name)

	raise ValueError('invalid dataset type %s' % type)

def find(dataset=None, max_depth=None, types=[]):
	netloc, path = _split_dataset(dataset) if dataset else (None, None)

	cmd = ['zfs', 'list']

	cmd.append('-H')

	if max_depth is None:
		cmd.append('-r')
	elif max_depth >= 0:
		cmd.append('-d')
		cmd.append(str(max_depth))
	else:
		raise TypeError('max_depth must be a non-negative int or None')

	if types:
		cmd.append('-t')
		cmd.append(','.join(types))

	cmd.append('-o')
	cmd.append('name,type')

	if path:
		cmd.append(path)

	return [_dataset(type, name) for name, type
		in process.check_output(cmd, netloc=netloc)]

def findprops(dataset=None, max_depth=None,
		props=['all'], sources=[], types=[]):
	netloc, path = _split_dataset(dataset) if dataset else (None, None)

	cmd = ['zfs', 'get']

	cmd.append('-H')
	cmd.append('-p')

	# workaround for lack of support for zfs get -t types in ZEVO:
	# use zfs list to find relevant datasets
	if True and types:
		paths = [dataset.name for dataset in
			find(dataset, max_depth=max_depth, types=types)]

		if not paths:
			return []
	else:
		if max_depth is None:
			cmd.append('-r')
		elif max_depth >= 0:
			cmd.append('-d')
			cmd.append(str(max_depth))
		else:
			raise TypeError('max_depth must be a non-negative int or None')

		if types:
			cmd.append('-t')
			cmd.append(','.join(types))

		paths = [path] if path else []

	if sources:
		cmd.append('-s')
		cmd.append(','.join(sources))

	cmd.append(','.join(props))

	cmd.extend(paths)

	return [dict(name=n, netloc=netloc, property=p, value=v, source=s)
		for n, p, v, s in process.check_output(cmd, netloc=netloc)]

def open(name, types=[]):
	return find(name, max_depth=0, types=types)[0]

def root_datasets():
	return find(max_depth=0)

# note: force means create missing parent filesystems
def create(name, type='filesystem', props={}, force=False):
		cmd = ['zfs', 'create']

		if type == 'volume':
			raise NotImplementedError()
		elif type != 'filesystem':
			raise ValueError('invalid type %s' % type)

		if force:
			cmd.append('-p')

		for prop, value in props.items():
			cmd.append('-o')
			cmd.append(prop + '=' + str(value))

		cmd.append(name)

		process.call(cmd)
		return ZFSFilesystem(name)

def receive(name, append_name=False, append_path=False,
		force=False, nomount=False, file=None):
	cmd = ['zfs', 'receive']

	if log.getEffectiveLevel() <= logging.INFO:
		cmd.append('-v')

	if append_name:
		cmd.append('-e')
	elif append_path:
		cmd.append('-d')

	if force:
		cmd.append('-F')
	if nomount:
		cmd.append('-u')

	cmd.append(name)

	# create and return pipe if no input file specified
	if file is None:
		return process.popen(cmd, mode='wb')
	else:
		process.call(cmd, stdin=file)

class ZFSDataset(object):
	def __init__(self, name):
		self.name = name

	def __str__(self):
		return self.name

	def parent(self):
		parent_name, _, _ = self.name.rpartition('/')
		return open(parent_name) if parent_name else None

	def filesystems(self):
		return find(self.name, max_depth=1, types=['filesystem'])[1:]

	def snapshots(self):
		return find(self.name, max_depth=1, types=['snapshot'])

	def children(self):
		return find(self.name, max_depth=1, types=['all'])[1:]

	def clones(self, recursive=False):
		raise NotImplementedError()

	def dependents(self, recursive=False):
		raise NotImplementedError()

	# TODO: split force to allow -f, -r and -R to be specified individually
	# TODO: remove or ignore defer option for non-snapshot datasets
	def destroy(self, defer=False, force=False):
		cmd = ['zfs', 'destroy']

		if defer:
			cmd.append('-d')

		if force:
			cmd.append('-f')
			cmd.append('-R')

		cmd.append(self.name)

		process.call(cmd)

	def snapshot(self, snapname, recursive=False, props={}):
		cmd = ['zfs', 'snapshot']

		if recursive:
			cmd.append('-r')

		for prop, value in props.items():
			cmd.append('-o')
			cmd.append(prop + '=' + str(value))

		name = self.name + '@' + snapname
		cmd.append(name)

		process.call(cmd)
		return ZFSSnapshot(name)

	# TODO: split force to allow -f, -r and -R to be specified individually
	def rollback(self, snapname, force=False):
		raise NotImplementedError()

	def promote(self):
		raise NotImplementedError()

	# TODO: split force to allow -f and -p to be specified individually
	def rename(self, name, recursive=False, force=False):
		raise NotImplementedError()

	def getprops(self):
		return findprops(self.name, max_depth=0)

	def getprop(self, prop):
		return findprops(self.name, max_depth=0, props=[prop])[0]

	def getpropval(self, prop, default=None):
		value = self.getprop(prop)['value']
		return default if value == '-' else value

	def setprop(self, prop, value):
		cmd = ['zfs', 'set']

		cmd.append(prop + '=' + str(value))
		cmd.append(self.name)

		process.call(cmd)

	def delprop(self, prop, recursive=False):
		cmd = ['zfs', 'inherit']

		if recursive:
			cmd.append('-r')

		cmd.append(prop)
		cmd.append(self.name)

		process.call(cmd)

	def userspace(self, *args, **kwargs):
		raise NotImplementedError()

	def groupspace(self, *args, **kwargs):
		raise NotImplementedError()

	def share(self, *args, **kwargs):
		raise NotImplementedError()

	def unshare(self, *args, **kwargs):
		raise NotImplementedError()

	def allow(self, *args, **kwargs):
		raise NotImplementedError()

	def unallow(self, *args, **kwargs):
		raise NotImplementedError()

class ZFSVolume(ZFSDataset):
	pass

class ZFSFilesystem(ZFSDataset):
	def upgrade(self, *args, **kwargs):
		raise NotImplementedError()

	def mount(self, *args, **kwargs):
		raise NotImplementedError()

	def unmount(self, *args, **kwargs):
		raise NotImplementedError()

class ZFSSnapshot(ZFSDataset):
	def snapname(self):
		_, _, snapname = self.name.rpartition('@')
		return snapname

	def parent(self):
		parent_name, _, _ = self.name.rpartition('@')
		return open(parent_name) if parent_name else None

	# note: force means create missing parent filesystems
	def clone(self, name, props={}, force=False):
		raise NotImplementedError()

	def send(self, base=None, intermediates=False, replicate=False,
			properties=False, deduplicate=False, file=None):
		cmd = ['zfs', 'send']

		if log.getEffectiveLevel() <= logging.INFO:
			cmd.append('-v')

		if replicate:
			cmd.append('-R')
		if properties:
			cmd.append('-p')
		if deduplicate:
			cmd.append('-D')

		if base is not None:
			if intermediates:
				cmd.append('-I')
			else:
				cmd.append('-i')
			cmd.append(base)

		cmd.append(self.name)

		# create and return pipe if no output file specified
		if file is None:
			return process.popen(cmd, mode='rb')
		else:
			process.call(cmd, stdout=file)

	def hold(self, tag, recursive=False):
		cmd = ['zfs', 'hold']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		process.call(cmd)

	def holds(self):
		cmd = ['zfs', 'holds']

		cmd.append('-H')

		cmd.append(self.name)

		# return hold tag names only
		return [hold[1] for hold in process.check_output(cmd)]

	def release(self, tag, recursive=False):
		cmd = ['zfs', 'release']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		process.call(cmd)
