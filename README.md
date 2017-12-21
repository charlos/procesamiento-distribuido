              ____
            /____ `\
           ||_  _`\ \
     .-.   `|O, O  ||
     | |    (/    -)\
     | |    |`-'` |\`
  __/  |    | _/  |
 (___) \.  _.\__. `\___
 (___)  )\/  \    _/  ~\.
 (___) . \   `--  _   `\
  (__)-    ,/        (   |
       `--~|         |   |
           |         |   | Torvalds

-----------------------------------------

# tp-2017-2c-LASTp

Como importar correctamente todos los proyectos a Eclipse:

## En la Terminal:

``git clone https://github.com/sisoputnfrba/so-commons-library.git /home/utnso/workspace/so-commons-library/``<br>
``git clone https://github.com/sisoputnfrba/tp-2017-1c-Stranger-Code.git /home/utnso/workspace/tp-2017-1c-Stranger-Code/``

## En Eclipse:

### para importar la commons
```
SI YA TENES LA COMMONS

Actualizar (haciendo un pull)

Compilar
```

```
SI NO TENES LA COMMONS

Si no tenes la view "Git Repositories"...
Ctrl+3 y escribir "Git Repositories" y agregar los dos repo en caso de que no figueren
Luego Git Repositories->so-commons-library->Working Directory->src
Clic Derecho->Import Projects->Import as general project
Poner en Project name: commons

Ahora tenemos que convertir al protecto en una Shared Library, para ello:
Clic Derecho sobre el proyecto commons->New->Convert to a C/C++ Project...
	Candidates for conversion: commons<br>
	Convert to C or C++: C Project
	Project options: Specify project type
	Project type: Shared Library
	Toolchains: Linux GCC
		
Poner flag -fPIC...
	Clic Derecho en proyecto commons->Properties->C/C++ Build->Settings
	Tool Settings->Miscellaneous->Position Independent Code (-fPIC)
```

### para importar el resto de los proyectos
```
Import->General->Existing Projects into Workspace
Luego en Select root directory: /home/utnso/workspace/tp-2017-1c-Stranger-Code
Seleccionamos todos los proyectos (kernel, shared library, etc)
```

**Si algo no se entiende o no te sale, preguntame!! NO intentar hacerlo de otra manera**
