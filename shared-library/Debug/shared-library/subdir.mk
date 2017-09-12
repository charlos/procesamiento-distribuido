################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../shared-library/data-node-prot.c \
../shared-library/file-system-prot.c \
../shared-library/socket.c 

OBJS += \
./shared-library/data-node-prot.o \
./shared-library/file-system-prot.o \
./shared-library/socket.o 

C_DEPS += \
./shared-library/data-node-prot.d \
./shared-library/file-system-prot.d \
./shared-library/socket.d 


# Each subdirectory must supply rules for building sources it contributes
shared-library/%.o: ../shared-library/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


