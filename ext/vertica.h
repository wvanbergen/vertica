#ifndef VERTICA_H
#define VERTICA_H

#include "ruby.h"
#include "libvq-fe.h"
#include "conn.h"

VALUE rb_Vertica;

VALUE rb_VerticaError;
VALUE rb_VerticaConnectionError;

VALUE rb_VerticaConnection;
VALUE rb_VerticaResult;

void Init_vertica();

#endif

