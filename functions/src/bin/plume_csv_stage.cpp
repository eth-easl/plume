#include "plume/functions/csv.hpp"

#include <iostream>

#ifdef __DANDELION__
# include "plume/abi/abi.hpp"
#else
# include "plume/abi/linux.hpp"
#endif

int main(int argc, char* argv[]) {
#ifndef __DANDELION__
    plume::linux::Init(argc, argv);
#endif

    auto status = plume::fn::RunCSVStage();
    if (status.is_error()) {
        auto error = status.error();
        int return_code = 0;

        switch (error.kind()) {
        case plume::ErrorKind::Generic:
            return_code = -1;
            break;
        case plume::ErrorKind::InvalidInput:
            return_code = -2;
            break;
        case plume::ErrorKind::NotImplemented:
            return_code = -3;
            break;
        case plume::ErrorKind::OutOfRange:
            return_code = -4;
            break;
        case plume::ErrorKind::RuntimeError:
            return_code = -5;
            break;
        }

        std::cerr << "Got Plume error: " << error.to_string() << std::endl;
        return return_code;
    }

#ifndef __DANDELION__
    plume::linux::Close();
#endif

    return 0;
}
