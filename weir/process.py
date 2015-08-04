import errno
import io
import logging
import re
import threading

from superprocess import Superprocess

log = logging.getLogger(__name__)

superprocess = Superprocess()

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

class CompletedProcess(superprocess.CompletedProcess):
	def check_returncode(self):
		# skip tests if return code is zero
		if not self.returncode:
			return

		# check for known errors
		if self.returncode == 1:
			# check for non-existent dataset
			pattern = r"^cannot open '([^']+)': dataset does not exist$"
			match = re.search(pattern, self.stderr)
			if match:
				raise DatasetNotFoundError(match.group(1))

			# check for existing dataset
			pattern = r"^cannot create \w+ '([^']+)': dataset already exists$"
			match = re.search(pattern, self.stderr)
			if match:
				raise DatasetExistsError(match.group(1))

			# check for busy dataset
			pattern = r"^cannot destroy '([^']+)': dataset is busy$"
			match = re.search(pattern, self.stderr)
			if match:
				raise DatasetBusyError(match.group(1))

			# check for non-existent hold tag
			pattern = r"^cannot release '[^']+' from '([^']+)': no such tag on this dataset$"
			match = re.search(pattern, self.stderr)
			if match:
				raise HoldTagNotFoundError(match.group(1))

			# check for existing hold tag
			pattern = r"^cannot hold '([^']+)': tag already exists on this dataset$"
			match = re.search(pattern, self.stderr)
			if match:
				raise HoldTagExistsError(match.group(1))

		# unrecognised error - defer to superclass
		super(CompletedProcess, self).check_returncode()

superprocess.CompletedProcess = CompletedProcess

class Popen(superprocess.Popen):
	def __init__(self, cmd, **kwargs):
		# zfs commands don't require setting both stdin and stdout
		stdin = kwargs.pop('stdin', None)
		stdout = kwargs.pop('stdout', None)
		if stdin is not None and stdout is not None:
			raise ValueError('only one of stdin or stdout may be set')

		# commands that accept input such as zfs receive may write
		# verbose output to stdout - redirect it to stderr
		if stdin is not None:
			stdout = superprocess.STDERR

		# use text mode by default
		universal_newlines = kwargs.pop('universal_newlines', True)

		# start process
		log.debug(' '.join(cmd))
		super(Popen, self).__init__(
			cmd, stdin=stdin, stdout=stdout, stderr=superprocess.PIPE,
			universal_newlines=universal_newlines, **kwargs)

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
		self.err_msg = None

	def communicate(self, *args, **kwargs):
		stdout, _ = super(Popen, self).communicate(*args, **kwargs)
		output = None if stdout is None else \
			[tuple(line.split('\t')) for line in stdout.splitlines()]
		self.err_thread.join()
		return output, self.err_msg

superprocess.Popen = Popen

check_call = superprocess.check_call
check_output = superprocess.check_output
popen = superprocess.popen
