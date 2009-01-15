require 'mkmf'

VERTICA_INSTALATION_PATH = '/opt/vertica'

if find_header('postgres_ext.h', VERTICA_INSTALATION_PATH + '/include') &&
   find_header('libpq-fe.h', VERTICA_INSTALATION_PATH + '/include/libpq') &&
   find_header('libvq-fe.h', VERTICA_INSTALATION_PATH + '/include/libvq') &&
   find_library('pq', 'PQescapeString', VERTICA_INSTALATION_PATH + '/lib') &&
   find_library('vq', 'VQconndefaults', VERTICA_INSTALATION_PATH + '/lib')
  create_makefile('vertica')
else
  puts 'Could not find Vertica build environment of libraries and header.  Makefile not created.'
end

