import logging
import subprocess

log = logging.getLogger(__name__)


# note: force means create missing parent filesystems
def create(name, type='filesystem', props={}, force=False):
	raise NotImplementedError()

def find(name=None, depth=None, types=['filesystem']):
	datasets = listprops(name, ['name'], depth=depth, types=types)
	return [ZFSDataset(dataset['name']) for dataset in datasets]

def root_datasets():
	return find(depth=0)

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

	# note: force means create missing parent filesystems
	def clone(self, name, props={}, force=False):
		raise NotImplementedError()

	def promote(self):
		raise NotImplementedError()

	# TODO: split force to allow -f and -p to be specified individually
	def rename(self, name, recursive=False, force=False):
		raise NotImplementedError()

def listprops(dataset, props, depth=0, types=[]):
	cmd = ['zfs', 'list']

	if depth >= 0:
		cmd.append('-d')
		cmd.append(str(depth))
	elif depth < 0:
		cmd.append('-r')

	if types:
		cmd.append('-t')
		cmd.append(','.join(types))

	cmd.append('-H')

	cmd.append('-o')
	cmd.append(','.join(props))

	if dataset:
		cmd.append(dataset)

	# execute command, capturing stdout and stderr
	log.debug(' '.join(cmd))
	out = subprocess.check_output(cmd)

	# return parsed list output
	rows = (line.split('\t') for line in out.splitlines())
	return [dict(zip(props, row)) for row in rows]

def setprop(dataset, prop, value):
	cmd = ['zfs', 'set']

	cmd.append(prop + '=' + str(value))
	cmd.append(dataset)

	log.debug(' '.join(cmd))
	subprocess.check_call(cmd)

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

def getprop(dataset, prop):
	return getprops(dataset, [prop])[0]

def getpropval(dataset, prop):
	return getprop(dataset, prop)[2]

def delprop(dataset, prop, recursive=False):
	cmd = ['zfs', 'inherit']

	if recursive:
		cmd.append('-r')

	cmd.append(prop)
	cmd.append(dataset)

	log.debug(' '.join(cmd))
	subprocess.check_call(cmd)

def upgrade(*args, **kwargs):
	raise NotImplementedError()

def userspace(*args, **kwargs):
	raise NotImplementedError()

def groupspace(*args, **kwargs):
	raise NotImplementedError()

def mount(*args, **kwargs):
	raise NotImplementedError()

def unmount(*args, **kwargs):
	raise NotImplementedError()

def share(*args, **kwargs):
	raise NotImplementedError()

def unshare(*args, **kwargs):
	raise NotImplementedError()

def send(volume, from_snapshot=None, to_snapshot=None, intermediates=False,
		replicate=False, properties=False, deduplicate=False):
	raise NotImplementedError()

def receive(name, append=None, force=False, nomount=False):
	raise NotImplementedError()

def allow(*args, **kwargs):
	raise NotImplementedError()

def unallow(*args, **kwargs):
	raise NotImplementedError()

def hold(snapshot, tag, recursive=False):
	cmd = ['zfs', 'hold']

	if recursive:
		cmd.append('-r')

	cmd.append(tag)
	cmd.append(snapshot)

	log.debug(' '.join(cmd))
	subprocess.check_call(cmd)

def holds(snapshot):
	cmd = ['zfs', 'holds']

	cmd.append('-H')

	cmd.append(snapshot)

	# execute command, capturing stdout and stderr
	log.debug(' '.join(cmd))
	out = subprocess.check_output(cmd)

	# return parsed output as list of hold tags
	return [line.split('\t')[1] for line in out.splitlines()]

def release(snapshot, tag, recursive=False):
	cmd = ['zfs', 'release']

	if recursive:
		cmd.append('-r')

	cmd.append(tag)
	cmd.append(snapshot)

	log.debug(' '.join(cmd))
	subprocess.check_call(cmd)
