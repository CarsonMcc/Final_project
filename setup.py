import subprocess
from distutils.command.build import build as _build
import setuptools

class build(_build):  
    sub_commands = _build.sub_commands + [('CustomCommands', None)]

class CustomCommands(setuptools.Command):

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout_data, _ = p.communicate()
        print('Command output: %s' % stdout_data)
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


CUSTOM_COMMANDS = [['pip', 'install','beam_nuggets']]

REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.32.0',
    'google-cloud-storage==1.41.0',
    'google-cloud-pubsub==2.7.0',
    'numpy==1.19.5'
    'beam_nuggets'
]

setuptools.setup(
    name='my-dataflow-pipeline',
    version='0.0.1',
    description='My Dataflow pipeline.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={'build': build, 'CustomCommands': CustomCommands}
)
