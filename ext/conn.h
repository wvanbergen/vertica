#ifndef CONN_H
#define CONN_H

#include "ruby.h"
#include "libvq-fe.h"
#include "vertica.h"

VALUE vtconn_alloc(VALUE klass);
void vtconn_free(VTconn *conn);
VALUE vtconn_init(int argc, VALUE *argv, VALUE self);
VALUE vtconn_poll(VALUE self);
VALUE vtconn_reset(int argc, VALUE *argv, VALUE self);
VALUE vtconn_reset_poll(VALUE self);
VALUE vtconn_finish(VALUE self);

VALUE vtconn_db(VALUE self);
VALUE vtconn_user(VALUE self);
VALUE vtconn_pass(VALUE self);
VALUE vtconn_host(VALUE self);
VALUE vtconn_port(VALUE self);
VALUE vtconn_options(VALUE self);
VALUE vtconn_status(VALUE self);
VALUE vtconn_transaction_status(VALUE self);
VALUE vtconn_parameter_status(VALUE self, VALUE parameter_name);
VALUE vtconn_protocol_version(VALUE self);
VALUE vtconn_server_version(VALUE self);
VALUE vtconn_error_message(VALUE self);
VALUE vtconn_socket(VALUE self);
VALUE vtconn_backend_pid(VALUE self);
VALUE vtconn_ssl_used(VALUE self);

#endif

