################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../shared-library/data-node-prot.o \
../shared-library/file-system-prot.o \
../shared-library/generales.o \
../shared-library/master-prot.o \
../shared-library/socket.o \
../shared-library/worker-prot.o \
../shared-library/yama-prot.o 

C_SRCS += \
../shared-library/data-node-prot.c \
../shared-library/file-system-prot.c \
../shared-library/generales.c \
../shared-library/master-prot.c \
../shared-library/socket.c \
../shared-library/worker-prot.c \
../shared-library/yama-prot.c 

OBJS += \
./shared-library/data-node-prot.o \
./shared-library/file-system-prot.o \
./shared-library/generales.o \
./shared-library/master-prot.o \
./shared-library/socket.o \
./shared-library/worker-prot.o \
./shared-library/yama-prot.o 

C_DEPS += \
./shared-library/data-node-prot.d \
./shared-library/file-system-prot.d \
./shared-library/generales.d \
./shared-library/master-prot.d \
./shared-library/socket.d \
./shared-library/worker-prot.d \
./shared-library/yama-prot.d 


# Each subdirectory must supply rules for building sources it contributes
shared-library/%.o: ../shared-library/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


