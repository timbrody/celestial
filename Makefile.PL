use 5.008005;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Celestial',
    VERSION_FROM      => 'lib/Celestial.pm', # finds $VERSION
		DISTNAME					=> 'OAI-PMH-Celestial',
		EXE_FILES					=>	['bin/celestial', 'bin/celestial-report'],
    PREREQ_PM         => {
			HTTP::OAI => 3.17,
			URI::Escape => 3.28,
		}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Celestial.pm', # retrieve abstract from module
       AUTHOR         => 'Timothy D Brody <tdb01r@localdomain>') : ()),
);
