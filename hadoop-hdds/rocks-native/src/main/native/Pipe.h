//
// Created by Swaminathan Balachandran on 3/2/23.
//

#ifndef UNTITLED_PIPE_H
#define UNTITLED_PIPE_H


#include <stdio.h>

class Pipe {
    public:
        Pipe();
        ~Pipe();
        void close();
        int getReadFd() {
            return p[0];
        }

        int getWriteFd() {
            return p[1];
        }

        bool isOpen() {
            return open;
        }


    private:
        int p[2];
        FILE* wr;
        bool open;

};


#endif //UNTITLED_PIPE_H
