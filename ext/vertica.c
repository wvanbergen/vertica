#include "ruby.h"
#include "libvq-fe.h"

#include "conn.h"

void Init_vertica() {
  rb_Vertica = rb_define_module("Vertica");

  rb_VerticaError = rb_define_class("Error", rb_eStandardError);

  rb_VerticaConnectionError = rb_define_class_under(rb_VerticaError, "ConnectionError", rb_VerticaError);
  rb_define_alias(rb_VerticaConnectionError, "error", "message");
  rb_define_attr(rb_VerticaConnectionError, "connection", 1, 0);
  rb_define_attr(rb_VerticaConnectionError, "result", 1, 0);

  /********************
   * VerticaConnection
   ********************/
  rb_VerticaConnection = rb_define_class_under(rb_Vertica, "Connection", rb_cObject);
  rb_define_alloc_func(rb_VerticaConnection, vtconn_alloc);
  rb_define_method(rb_VerticaConnection, "initialize", vtconn_init, -1);
  rb_define_method(rb_VerticaConnection, "poll", vtconn_poll, 0);
  rb_define_method(rb_VerticaConnection, "finish", vtconn_finish, 0);
  rb_define_alias(rb_VerticaConnection, "close", "finish");
  rb_define_method(rb_VerticaConnection, "reset", vtconn_reset, -1);
  rb_define_method(rb_VerticaConnection, "reset_poll", vtconn_reset_poll, 0);
  rb_define_method(rb_VerticaConnection, "db", vtconn_db, 0);
  rb_define_method(rb_VerticaConnection, "user", vtconn_user, 0);
  rb_define_method(rb_VerticaConnection, "password", vtconn_pass, 0);
  rb_define_method(rb_VerticaConnection, "host", vtconn_host, 0);
  rb_define_method(rb_VerticaConnection, "port", vtconn_port, 0);
  rb_define_method(rb_VerticaConnection, "options", vtconn_options, 0);
  rb_define_method(rb_VerticaConnection, "status", vtconn_status, 0);
  rb_define_method(rb_VerticaConnection, "parameter_status", vtconn_parameter_status, 1);
  rb_define_method(rb_VerticaConnection, "transaction_status", vtconn_transaction_status, 0);
  rb_define_method(rb_VerticaConnection, "protocol_version", vtconn_protocol_version, 0);
  rb_define_method(rb_VerticaConnection, "server_version", vtconn_server_version, 0);
  rb_define_method(rb_VerticaConnection, "error_message", vtconn_error_message, 0);
  rb_define_method(rb_VerticaConnection, "socket", vtconn_socket, 0);
  rb_define_method(rb_VerticaConnection, "backend_pid", vtconn_backend_pid, 0);
  rb_define_method(rb_VerticaConnection, "ssl?", vtconn_ssl_used, 0);

}

