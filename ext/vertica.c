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
  rb_define_method(rb_VerticaConnection, "initialize", vtconn_init, 6);
  rb_define_method(rb_VerticaConnection, "finish", vtconn_finish, 0);
  rb_define_alias(rb_VerticaConnection, "close", "finish");
  rb_define_method(rb_VerticaConnection, "reset", vtconn_reset, 0);

}

