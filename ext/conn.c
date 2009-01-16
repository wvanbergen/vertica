#include "conn.h"

VTconn* get_vtconn(VALUE self) {
  VTconn *conn;
  Data_Get_Struct(self, VTconn, conn);
  if (conn == NULL) rb_raise(rb_VerticaConnectionError, "not connected");
  return conn;
}

VALUE vtconn_alloc(VALUE klass) {
  return Data_Wrap_Struct(klass, NULL, vtconn_free, NULL);
}

void vtconn_free(VTconn *conn) {
  VQfinish(conn);
}

char *checked_string_value_ptr(VALUE string) {
  if (!NIL_P(string)) {
    Check_Type(string, T_STRING);
    return StringValuePtr(string);
  }
  return NULL;
}

VALUE vtconn_init(VALUE self, VALUE host, VALUE port, VALUE opts, VALUE dbname, VALUE username, VALUE password) {
  VTconn *conn = VQsetdbLogin(checked_string_value_ptr(host),
                              checked_string_value_ptr(rb_obj_as_string(port)),
                              checked_string_value_ptr(opts),
                              NULL,
                              checked_string_value_ptr(dbname),
                              checked_string_value_ptr(username),
                              checked_string_value_ptr(password));

  if (VQstatus(conn) == CONNECTION_BAD) {
    VALUE error = rb_exc_new2(rb_VerticaConnectionError, VQerrorMessage(conn));
    rb_iv_set(error, "@connection", self);
    rb_exc_raise(error);
  }

  Check_Type(self, T_DATA);
  DATA_PTR(self) = conn;

  return self;
}

VALUE vtconn_finish(VALUE self) {
  VQfinish(get_vtconn(self));
   DATA_PTR(self) = NULL;
   return Qnil;
}

VALUE vtconn_reset(VALUE self) {
  VQreset(get_vtconn(self));
  return self;
}

