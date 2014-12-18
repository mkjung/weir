import errno
import logging
import re
import subprocess

log = logging.getLogger(__name__)

# Wait for process to complete and check result
def check_result(p):
	out, err = (s.strip() if s else s for s in p.communicate())
	retcode = p.wait()

	# raise OSError if dataset not found
	if retcode == 1:
		match = re.search("^cannot open '([^']+)': dataset does not exist$", err)
		if match:
			dataset = match.group(1)
			raise OSError(errno.ENOENT, err, dataset)

	# raise CalledProcessError for any other error
	if retcode:
		raise subprocess.CalledProcessError(retcode, 'zfs', output=err)

	# log verbose output at INFO level
	if err:
		for line in err.splitlines():
			log.info(line)

	return out

# Replacement for subprocess.check_call() that uses check_result()
def check_call(*popenargs, **kwargs):
	p = subprocess.Popen(*popenargs, stderr=subprocess.PIPE, **kwargs)
	check_result(p)
	return 0

# Replacement for subprocess.check_output() that uses check_result()
def check_output(*popenargs, **kwargs):
	PIPE = subprocess.PIPE
	p = subprocess.Popen(*popenargs, stdout=PIPE, stderr=PIPE, **kwargs)
	return check_result(p)

# Low level wrapper around zfs get command
def _get(datasets, props, depth=0, sources=[]):
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

	cmd.extend(datasets)

	# execute command, capturing stdout
	log.debug(' '.join(cmd))
	out = check_output(cmd)

	# return parsed output as list of (name, property, value, source) tuples
	return [tuple(line.split('\t')) for line in out.splitlines()]

# Low level wrapper around zfs list command
def _list(datasets, props, depth=0, types=[]):
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

	cmd.extend(datasets)

	# execute command, capturing stdout
	log.debug(' '.join(cmd))
	out = check_output(cmd)

	# return parsed output as list of dicts
	rows = (line.split('\t') for line in out.splitlines())
	return [dict(zip(props, row)) for row in rows]

# Internal factory function to instantiate dataset object
def _dataset(type, name):
	if type == 'volume':
		return ZFSVolume(name)

	if type == 'filesystem':
		return ZFSFilesystem(name)

	if type == 'snapshot':
		return ZFSSnapshot(name)

	raise ValueError('invalid dataset type %s' % type)

def find(*paths, **kwargs):
	depth = kwargs.get('depth', None)
	types = kwargs.get('types', ['all'])
	datasets = _list(paths, ('name', 'type'), depth=depth, types=types)
	return [_dataset(d['type'], d['name']) for d in datasets]

def open(name, types=[]):
	return find(name, depth=0, types=types)[0]

def root_datasets():
	return find(depth=0)

# note: force means create missing parent filesystems
def create(name, type='filesystem', props={}, force=False):
		cmd = ['zfs', 'create']

		if type == 'volume':
			raise NotImplementedError()
		elif type != 'filesystem':
			raise ValueError('invalid type %s' % type)

		if force:
			cmd.append('-p')

		for prop, value in props.iteritems():
			cmd.append('-o')
			cmd.append(prop + '=' + str(value))

		cmd.append(name)

		log.debug(' '.join(cmd))
		check_call(cmd)
		return ZFSFilesystem(name)

def receive_async(name, append_name=False, append_path=False,
		force=False, nomount=False, stdin=None):
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

	# zfs receive writes verbose output to stdout, so redirect stderr
	# to stdout and swap so all logged info goes to stderr as expected
	log.debug(' '.join(cmd))
	p = subprocess.Popen(cmd, stdin=stdin,
		stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	p.stdout, p.stderr = None, p.stdout
	return p

def receive(*args, **kwargs):
	p = receive_async(*args, **kwargs)
	check_result(p)

class ZFSDataset(object):
	def __init__(self, name):
		self.name = name

	def __str__(self):
		return self.name

	def parent(self):
		parent_name, _, _ = self.name.rpartition('/')
		return open(parent_name) if parent_name else None

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
	# TODO: remove or ignore defer option for non-snapshot datasets
	def destroy(self, defer=False, force=False):
		cmd = ['zfs', 'destroy']

		if defer:
			cmd.append('-d')

		if force:
			cmd.append('-f')
			cmd.append('-R')

		cmd.append(self.name)

		log.debug(' '.join(cmd))
		check_call(cmd)

	def snapshot(self, snapname, recursive=False, props={}):
		cmd = ['zfs', 'snapshot']

		if recursive:
			cmd.append('-r')

		for prop, value in props.iteritems():
			cmd.append('-o')
			cmd.append(prop + '=' + str(value))

		name = self.name + '@' + snapname
		cmd.append(name)

		log.debug(' '.join(cmd))
		check_call(cmd)
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
		return _get([self.name], ['all'])

	def getprop(self, prop):
		return _get([self.name], [prop])[0]

	def getpropval(self, prop, default=None):
		value = self.getprop(prop)[2]
		return default if value == '-' else value

	def setprop(self, prop, value):
		cmd = ['zfs', 'set']

		cmd.append(prop + '=' + str(value))
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		check_call(cmd)

	def delprop(self, prop, recursive=False):
		cmd = ['zfs', 'inherit']

		if recursive:
			cmd.append('-r')

		cmd.append(prop)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		check_call(cmd)

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

	def send_async(self, base=None, intermediates=False, replicate=False,
			properties=False, deduplicate=False, stdout=None):
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

		log.debug(' '.join(cmd))
		return subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.PIPE)

	def send(self, *args, **kwargs):
		p = self.send_async(*args, **kwargs)
		check_result(p)

	def hold(self, tag, recursive=False):
		cmd = ['zfs', 'hold']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		check_call(cmd)

	def holds(self):
		cmd = ['zfs', 'holds']

		cmd.append('-H')

		cmd.append(self.name)

		# execute command, capturing stdout and stderr
		log.debug(' '.join(cmd))
		out = check_output(cmd)

		# return parsed output as list of hold tags
		return [line.split('\t')[1] for line in out.splitlines()]

	def release(self, tag, recursive=False):
		cmd = ['zfs', 'release']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		check_call(cmd)
