################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../shared-library/connect.c \
../shared-library/file_system_prot.c \
../shared-library/generales.c \
../shared-library/memory_prot.c \
../shared-library/serializar.c \
../shared-library/socket.c 

OBJS += \
./shared-library/connect.o \
./shared-library/file_system_prot.o \
./shared-library/generales.o \
./shared-library/memory_prot.o \
./shared-library/serializar.o \
./shared-library/socket.o 

C_DEPS += \
./shared-library/connect.d \
./shared-library/file_system_prot.d \
./shared-library/generales.d \
./shared-library/memory_prot.d \
./shared-library/serializar.d \
./shared-library/socket.d 


# Each subdirectory must supply rules for building sources it contributes
shared-library/%.o: ../shared-library/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -fPIC -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


