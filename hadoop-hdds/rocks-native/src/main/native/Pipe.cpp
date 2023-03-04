//
// Created by Swaminathan Balachandran on 3/2/23.
//

#include "Pipe.h"
#include <unistd.h>

Pipe::Pipe() {
    pipe(p);
    open = true;
}

Pipe::~Pipe() {
    ::close(p[0]);
    ::close(p[1]);
}

void Pipe::close() {
    open = false;
}
