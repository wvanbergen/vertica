#ifndef CONN_H
#define CONN_H

#include "ruby.h"
#include "libvq-fe.h"
#include "vertica.h"

VALUE vtconn_alloc(VALUE klass);
void vtconn_free(VTconn *conn);
VALUE vtconn_init(VALUE host, VALUE port, VALUE opts, VALUE dbname, VALUE username, VALUE password, VALUE self);
VALUE vtconn_reset(VALUE self);
VALUE vtconn_finish(VALUE self);

#endif

