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

VALUE vtconn_init(int argc, VALUE *argv, VALUE self) {
  VALUE args;
  VTconn *conn;
  
  rb_scan_args(argc, argv, "0*", &args);
  if (RARRAY(args)->len == 1) {
    conn = VQconnectdb(checked_string_value_ptr(rb_ary_entry(args, 0)));
  } else if (RARRAY(args)->len == 2) {
    char *conn_info = checked_string_value_ptr(rb_ary_entry(args, 0));
    VALUE async_value = rb_ary_entry(args, 1);
    if (!NIL_P(async_value) && async_value == Qtrue) {
      conn = VQconnectStart(conn_info);
    } else {
      conn = VQconnectdb(conn_info);
    }
  } else if (RARRAY(args)->len == 6) {
    conn = VQsetdbLogin(checked_string_value_ptr(rb_ary_entry(args, 0)),
                                checked_string_value_ptr(rb_obj_as_string(rb_ary_entry(args, 1))),
                                checked_string_value_ptr(rb_ary_entry(args, 2)),
                                NULL,
                                checked_string_value_ptr(rb_ary_entry(args, 3)),
                                checked_string_value_ptr(rb_ary_entry(args, 4)),
                                checked_string_value_ptr(rb_ary_entry(args, 5)));
  } else {
    rb_raise(rb_eArgError, "Expected connection info string 6 separate string arguments.");
  }

  if (VQstatus(conn) == CONNECTION_BAD) {
    VALUE error = rb_exc_new2(rb_VerticaConnectionError, VQerrorMessage(conn));
    rb_iv_set(error, "@connection", self);
    rb_exc_raise(error);
  }

  Check_Type(self, T_DATA);
  DATA_PTR(self) = conn;

  return self;
}

VALUE polling_status_type_to_sym(VerticaPollingStatusType polling_status) {
  switch(polling_status) {
    case VERT_POLLING_FAILED:
      return ID2SYM(rb_intern("failed"));
    case VERT_POLLING_READING:
      return ID2SYM(rb_intern("reading"));
    case VERT_POLLING_WRITING:
      return ID2SYM(rb_intern("writing"));
    case VERT_POLLING_OK:
      return ID2SYM(rb_intern("ok"));
    case VERT_POLLING_ACTIVE:
      return ID2SYM(rb_intern("active"));
    default:
      return Qnil;
  }
}

VALUE vtconn_poll(VALUE self) {
  VerticaPollingStatusType polling_status = VQconnectPoll(get_vtconn(self));
  return polling_status_type_to_sym(polling_status);
}

VALUE vtconn_finish(VALUE self) {
  VQfinish(get_vtconn(self));
   DATA_PTR(self) = NULL;
   return Qnil;
}

VALUE vtconn_reset(int argc, VALUE *argv, VALUE self) {
  VALUE args;

  rb_scan_args(argc, argv, "0*", &args);
  if (RARRAY(args)->len == 1 && !NIL_P(rb_ary_entry(args, 0)) && rb_ary_entry(args, 0) == Qtrue) {
    VQresetStart(get_vtconn(self));
  } else {
    VQreset(get_vtconn(self));
  }

  return self;
}

VALUE vtconn_reset_poll(VALUE self) {
  VerticaPollingStatusType polling_status = VQresetPoll(get_vtconn(self));
  return polling_status_type_to_sym(polling_status);
}

VALUE vtconn_db(VALUE self) {
  return rb_str_new2(VQdb(get_vtconn(self)));
}

VALUE vtconn_user(VALUE self) {
  return rb_str_new2(VQuser(get_vtconn(self)));
}

VALUE vtconn_pass(VALUE self) {
  return rb_str_new2(VQpass(get_vtconn(self)));
}

VALUE vtconn_host(VALUE self) {
  return rb_str_new2(VQhost(get_vtconn(self)));
}

VALUE vtconn_port(VALUE self) {
  return rb_str_new2(VQport(get_vtconn(self)));
}

VALUE vtconn_options(VALUE self) {
  return rb_str_new2(VQoptions(get_vtconn(self)));
}

VALUE conn_status_type_to_sym(ConnStatusType conn_status) {
  switch(conn_status) {
    case CONNECTION_OK:
      return ID2SYM(rb_intern("ok"));
    case CONNECTION_BAD:
      return ID2SYM(rb_intern("bad"));
    case CONNECTION_STARTED:
      return ID2SYM(rb_intern("started"));
    case CONNECTION_MADE:
      return ID2SYM(rb_intern("made"));
    case CONNECTION_AWAITING_RESPONSE:
      return ID2SYM(rb_intern("awaiting_response"));
    case CONNECTION_AUTH_OK:
      return ID2SYM(rb_intern("auth_ok"));
    case CONNECTION_SETENV:
      return ID2SYM(rb_intern("setenv"));
    case CONNECTION_SSL_STARTUP:
      return ID2SYM(rb_intern("ssl_startup"));
    case CONNECTION_NEEDED:
      return ID2SYM(rb_intern("needed"));
    default:
      return Qnil;
  }
}

VALUE vtconn_status(VALUE self) {
  return conn_status_type_to_sym(VQstatus(get_vtconn(self)));
}

VALUE transaction_status_type_to_sym(VTTransactionStatusType transaction_status) {
  switch(transaction_status) {
    case VQTRANS_IDLE:
      return ID2SYM(rb_intern("idle"));
    case VQTRANS_ACTIVE:
      return ID2SYM(rb_intern("active"));
    case VQTRANS_INTRANS:
      return ID2SYM(rb_intern("in_transaction"));
    case VQTRANS_INERROR:
      return ID2SYM(rb_intern("in_error"));
    case VQTRANS_UNKNOWN:
      return ID2SYM(rb_intern("unknown"));
    default:
      return Qnil;
  }
}

VALUE vtconn_transaction_status(VALUE self) {
  return transaction_status_type_to_sym(VQtransactionStatus(get_vtconn(self)));
}

VALUE vtconn_parameter_status(VALUE self, VALUE parameter_name) {
  Check_Type(parameter_name, T_STRING);
  const char *result = VQparameterStatus(get_vtconn(self), StringValuePtr(parameter_name));
  if (result == NULL) {
    return Qnil;
  } else {
    return rb_str_new2(result);
  }
}

VALUE vtconn_protocol_version(VALUE self) {
  return INT2NUM(VQprotocolVersion(get_vtconn(self)));
}

VALUE vtconn_server_version(VALUE self) {
  return INT2NUM(VQserverVersion(get_vtconn(self)));
}

VALUE vtconn_error_message(VALUE self) {
  return rb_str_new2(VQerrorMessage(get_vtconn(self)));
}

VALUE vtconn_socket(VALUE self) {
  return INT2NUM(VQsocket(get_vtconn(self)));
}

VALUE vtconn_backend_pid(VALUE self) {
  return INT2NUM(VQbackendPID(get_vtconn(self)));
}

VALUE vtconn_ssl_used(VALUE self) {
  return VQgetssl(get_vtconn(self)) == NULL ? Qfalse : Qtrue;
}

