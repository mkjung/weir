import logging
import subprocess

log = logging.getLogger(__name__)


def getprops(dataset, props, depth=0, sources=[]):
	cmd = ['zfs', 'get']

	if depth > 0:
		cmd.append('-d')
		cmd.append(str(depth))
	elif depth < 0:
		cmd.append('-r')

	if sources:
		cmd.append('-s')
		cmd.append(','.join(sources))

	cmd.append('-H')
	cmd.append('-p')

	cmd.append(','.join(props))

	cmd.append(dataset)

	# execute command, capturing stdout and stderr
	log.debug(' '.join(cmd))
	out = subprocess.check_output(cmd)

	# return parsed output as list of (name, property, value, source) tuples
	return [tuple(line.split('\t')) for line in out.splitlines()]

def find(*paths, **kwargs):
	cmd = ['zfs', 'list']

	depth = kwargs.get('depth', None)
	if depth >= 0:
		cmd.append('-d')
		cmd.append(str(depth))
	elif depth < 0:
		cmd.append('-r')

	types = kwargs.get('types', ['all'])
	if types:
		cmd.append('-t')
		cmd.append(','.join(types))

	cmd.append('-H')

	cmd.append('-o')
	cmd.append('name,type')

	for path in paths:
		cmd.append(path)

	# execute command, capturing stdout and stderr
	log.debug(' '.join(cmd))
	out = subprocess.check_output(cmd)
	rows = (line.split('\t') for line in out.splitlines())
	return [open(name, type) for name, type in rows]

def root_datasets():
	return find(depth=0)

def open(name, type=None):
	if type == 'volume':
		return ZFSVolume(name)

	if type == 'filesystem':
		return ZFSFilesystem(name)

	if type == 'snapshot':
		return ZFSSnapshot(name)

	raise ValueError('invalid dataset type %s' % type)

# note: force means create missing parent filesystems
def create(name, type='filesystem', props={}, force=False):
	raise NotImplementedError()

def receive(name, append=None, force=False, nomount=False):
	raise NotImplementedError()

class ZFSDataset(object):
	def __init__(self, name):
		self.name = name

	def __str__(self):
		return self.name

	def filesystems(self, recursive=False):
		depth = None if recursive else 1
		return find(self.name, depth=depth, types=['filesystem'])[1:]

	def snapshots(self, recursive=False):
		depth = None if recursive else 1
		return find(self.name, depth=depth, types=['snapshot'])

	def children(self, recursive=False):
		depth = None if recursive else 1
		return find(self.name, depth=depth, types=['all'])[1:]

	def clones(self, recursive=False):
		raise NotImplementedError()

	def dependents(self, recursive=False):
		raise NotImplementedError()

	# TODO: split force to allow -f, -r and -R to be specified individually
	def destroy(self, defer=False, force=False):
		raise NotImplementedError()

	def snapshot(self, snapname, props={}, recursive=False):
		raise NotImplementedError()

	# TODO: split force to allow -f, -r and -R to be specified individually
	def rollback(self, snapname, force=False):
		raise NotImplementedError()

	def promote(self):
		raise NotImplementedError()

	# TODO: split force to allow -f and -p to be specified individually
	def rename(self, name, recursive=False, force=False):
		raise NotImplementedError()

	def getprops(self):
		return getprops(self.name, ['all'])

	def getprop(self, prop):
		return getprops(self.name, [prop])[0]

	def getpropval(self, prop):
		return self.getprop(prop)[2]

	def setprop(self, prop, value):
		cmd = ['zfs', 'set']

		cmd.append(prop + '=' + str(value))
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		subprocess.check_call(cmd)

	def delprop(self, prop, recursive=False):
		cmd = ['zfs', 'inherit']

		if recursive:
			cmd.append('-r')

		cmd.append(prop)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		subprocess.check_call(cmd)

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
	# note: force means create missing parent filesystems
	def clone(self, name, props={}, force=False):
		raise NotImplementedError()

	def send(self, from_snapshot=None, intermediates=False,
			replicate=False, properties=False, deduplicate=False):
		raise NotImplementedError()

	def hold(self, tag, recursive=False):
		cmd = ['zfs', 'hold']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		subprocess.check_call(cmd)

	def holds(self):
		cmd = ['zfs', 'holds']

		cmd.append('-H')

		cmd.append(self.name)

		# execute command, capturing stdout and stderr
		log.debug(' '.join(cmd))
		out = subprocess.check_output(cmd)

		# return parsed output as list of hold tags
		return [line.split('\t')[1] for line in out.splitlines()]

	def release(self, tag, recursive=False):
		cmd = ['zfs', 'release']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		subprocess.check_call(cmd)
