import errno
import io
import logging
import re
import threading

import superprocess

log = logging.getLogger(__name__)

PIPE = superprocess.PIPE
STDOUT = superprocess.PIPE
STDERR = superprocess.STDERR
CalledProcessError = superprocess.CalledProcessError

class DatasetNotFoundError(OSError):
	def __init__(self, dataset):
		super(DatasetNotFoundError, self).__init__(
			errno.ENOENT, 'dataset does not exist', dataset)

class DatasetExistsError(OSError):
	def __init__(self, dataset):
		super(DatasetExistsError, self).__init__(
			errno.EEXIST, 'dataset already exists', dataset)

class DatasetBusyError(OSError):
	def __init__(self, dataset):
		super(DatasetBusyError, self).__init__(
			errno.EBUSY, 'dataset is busy', dataset)

class HoldTagNotFoundError(OSError):
	def __init__(self, dataset):
		super(HoldTagNotFoundError, self).__init__(
			errno.ENOENT, 'no such tag on this dataset', dataset)

class HoldTagExistsError(OSError):
	def __init__(self, dataset):
		super(HoldTagExistsError, self).__init__(
			errno.EEXIST, 'tag already exists on this dataset', dataset)

class Popen(superprocess.Popen):
	@classmethod
	def check_output(cls, cmd, **kwargs):
		output = super(Popen, cls).check_output(
			cmd, universal_newlines=True, **kwargs)

		return [tuple(line.split('\t')) for line in output.splitlines()]

	def __init__(self, cmd, **kwargs):
		# zfs commands don't require setting both stdin and stdout
		stdin = kwargs.pop('stdin', None)
		stdout = kwargs.pop('stdout', None)
		if stdin is not None and stdout is not None:
			raise ValueError('only one of stdin or stdout may be set')

		# commands that accept input such as zfs receive may write
		# verbose output to stdout - redirect it to stderr
		if stdin is not None:
			stdout = STDERR

		# fail on error by default
		fail_on_error = kwargs.pop('fail_on_error', True)

		# start process
		log.debug(' '.join(cmd))
		super(Popen, self).__init__(
			cmd, stdin=stdin, stdout=stdout, stderr=PIPE,
			fail_on_error=fail_on_error, **kwargs)

		# set stderr aside for logging and ensure it is a text stream
		stderr, self.stderr = self.stderr, None
		if not isinstance(stderr, io.TextIOBase):
			stderr = io.TextIOWrapper(stderr)

		# set log level
		if '-v' in cmd:
			# set log level to INFO for commands that output verbose
			# info (send, receive, destroy, mount, upgrade)
			log_level = logging.INFO
		else:
			# most commands only write to stderr on failure - in which case an
			# exception will be generated and it's sufficient to log at DEBUG
			log_level = logging.DEBUG

		# write stderr to log and store most recent line for analysis
		def log_stderr():
			with stderr as f:
				for line in f:
					msg = line.strip()
					log.log(log_level, msg)
					self.err_msg = msg
		t = threading.Thread(target=log_stderr)
		t.daemon = True
		t.start()
		self.err_thread = t

	def check(self):
		# skip tests if return code is zero or not set yet
		if not self.returncode:
			return

		# wait for stderr reader thread to finish
		self.err_thread.join()

		# check for known errors
		if self.returncode == 1:
			# check for non-existent dataset
			pattern = r"^cannot open '([^']+)': dataset does not exist$"
			match = re.search(pattern, self.err_msg)
			if match:
				raise DatasetNotFoundError(match.group(1))

			# check for existing dataset
			pattern = r"^cannot create \w+ '([^']+)': dataset already exists$"
			match = re.search(pattern, self.err_msg)
			if match:
				raise DatasetExistsError(match.group(1))

			# check for busy dataset
			pattern = r"^cannot destroy '([^']+)': dataset is busy$"
			match = re.search(pattern, self.err_msg)
			if match:
				raise DatasetBusyError(match.group(1))

			# check for non-existent hold tag
			pattern = r"^cannot release '[^']+' from '([^']+)': no such tag on this dataset$"
			match = re.search(pattern, self.err_msg)
			if match:
				raise HoldTagNotFoundError(match.group(1))

			# check for existing hold tag
			pattern = r"^cannot hold '([^']+)': tag already exists on this dataset$"
			match = re.search(pattern, self.err_msg)
			if match:
				raise HoldTagExistsError(match.group(1))

		# unrecognised error - defer to superclass
		super(Popen, self).check()

call = Popen.call
check_call = Popen.check_call
check_output = Popen.check_output
popen = Popen.popen
