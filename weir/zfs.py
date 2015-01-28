import errno
import logging
import os
import re
import subprocess
import threading

log = logging.getLogger(__name__)

# Variant of os.popen() that works on an existing process
def popen(p, mode=None):
	if mode is None:
		f = p.stdout or p.stdin
	elif mode == 'r':
		f = p.stdout
	elif mode == 'w':
		f = p.stdin
	else:
		raise ValueError('invalid mode %s' % mode)

	if not f:
		raise ValueError('process has no pipe for mode %s' % mode)

	return PopenFile(f, p)

# Helper for popen() - wraps file so that it waits for process when closed
class PopenFile(object):
	def __init__(self, file, process):
		self.file = file
		self.process = process

	def __getattr__(self, name):
		return getattr(self.file, name)

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()

	def __iter__(self):
		return self

	def next(self):
		return self.file.next()

	def close(self):
		self.file.close()
		returncode = self.process.wait()
		if returncode:
			return returncode

# Subclass of subprocess.Popen that raises an exception instead
# of returning a non-zero value from poll() or wait().
class Process(subprocess.Popen):
	PIPE = subprocess.PIPE      # -1
	STDOUT = subprocess.STDOUT  # -2
	STDERR = STDOUT - 1         # -3

	def __init__(self, cmd, stdin=None, stdout=None, stderr=None, **kwargs):
		# use stderr=STDOUT to combine streams for stdout=STDERR
		redir_stdout = (stdout == Process.STDERR)
		if redir_stdout:
			stdout, stderr = stderr, Process.STDOUT

		# initialise process
		super(Process, self).__init__(
			cmd, stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
		self.cmd = cmd

		# move output to stderr for stdout=STDERR
		if redir_stdout:
			self.stdout, self.stderr = None, self.stdout

	def check(self):
		if self.returncode:
			raise subprocess.CalledProcessError(self.returncode, self.cmd)

	def poll(self):
		super(Process, self).poll()
		self.check()

	def wait(self):
		super(Process, self).wait()
		self.check()

# Return a readable file object wrapping an iterable
# Uses os.pipe() to create a real file
def iteropen(iterable):
	# Wrapper for file.writelines that closes file and sequence when done
	def writelines(file, sequence):
		try:
			try:
				file.writelines(sequence)
			finally:
				file.close()
		finally:
			if hasattr(sequence, 'close'): sequence.close()

	piperead, pipewrite = os.pipe()
	try:
		# TODO: should set CLOEXEC on both fds
		piperead = os.fdopen(piperead, 'rb')
		pipewrite = os.fdopen(pipewrite, 'wb')

		# copy data in background
		t = threading.Thread(target=writelines, args=(pipewrite, iterable,))
		t.daemon = True
		t.start()

		return piperead
	except:
		for f in piperead, pipewrite:
			try: os.close(f) if isinstance(f, int) else f.close()
			except: pass
		raise

# Write stderr of running process directly to log at INFO level
def log_stderr(p):
	def lines(stderr):
		# XXX: try / finally not entered until first value requested
		# (but unlikely to result in stderr being left open here)
		try:
			for line in iter(stderr.readline, ''):
				if not p.poll():
					log.info(line.strip())
				else:
					yield line
		finally:
			stderr.close()
	p.stderr = iteropen(lines(p.stderr))

# Run a zfs command and wait for it to complete
def zfs_call(cmd, **kwargs):
	p = Process(cmd, stderr=Process.PIPE, **kwargs)
	log_stderr(p)
	return p.wait()

# Run a zfs command and return its output
def zfs_output(cmd, **kwargs):
	p = Process(cmd, stdout=Process.PIPE, stderr=Process.PIPE, **kwargs)
	log_stderr(p)
	return p.communicate()[0].strip()

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
	out = zfs_output(cmd)

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
	out = zfs_output(cmd)

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
		zfs_call(cmd)
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

	# create pipe and return process immediately if no input file specified
	# note: zfs receive writes verbose output to stdout, so redirect to stderr
	log.debug(' '.join(cmd))
	if file is None:
		p = Process(cmd, stdin=Process.PIPE, stdout=Process.STDERR, stderr=Process.PIPE)
		log_stderr(p)
		return popen(p)
	else:
		zfs_call(cmd, stdin=file, stdout=Process.STDERR)

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
		zfs_call(cmd)

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
		zfs_call(cmd)
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
		zfs_call(cmd)

	def delprop(self, prop, recursive=False):
		cmd = ['zfs', 'inherit']

		if recursive:
			cmd.append('-r')

		cmd.append(prop)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		zfs_call(cmd)

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

		log.debug(' '.join(cmd))

		# create pipe and return process immediately if no output file specified
		if file is None:
			p = Process(cmd, stdout=Process.PIPE, stderr=Process.PIPE)
			log_stderr(p)
			return popen(p)
		else:
			zfs_call(cmd, stdout=file)

	def hold(self, tag, recursive=False):
		cmd = ['zfs', 'hold']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		zfs_call(cmd)

	def holds(self):
		cmd = ['zfs', 'holds']

		cmd.append('-H')

		cmd.append(self.name)

		# execute command, capturing stdout and stderr
		log.debug(' '.join(cmd))
		out = zfs_output(cmd)

		# return parsed output as list of hold tags
		return [line.split('\t')[1] for line in out.splitlines()]

	def release(self, tag, recursive=False):
		cmd = ['zfs', 'release']

		if recursive:
			cmd.append('-r')

		cmd.append(tag)
		cmd.append(self.name)

		log.debug(' '.join(cmd))
		zfs_call(cmd)
