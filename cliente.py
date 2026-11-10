"""
PFO 3 - Rediseño como Sistema Distribuido (Cliente-Servidor)
Cliente: envía tareas al servidor por socket TCP y recibe los resultados.

Los resultados llegan a medida que cada worker termina, no necesariamente
en el orden en que se enviaron las tareas. Por eso un hilo aparte escucha
las respuestas mientras el hilo principal sigue enviando.

Uso:
  python cliente.py            # menú interactivo
  python cliente.py --demo     # envía un lote de tareas de ejemplo
"""
import argparse
import json
import socket
import sys
import threading
import time

TIPOS = {
    "1": ("eco", "texto"),
    "2": ("mayusculas", "texto"),
    "3": ("contar_palabras", "texto"),
    "4": ("suma", "números separados por espacio"),
    "5": ("factorial", "número entero (0 a 1000)"),
    "6": ("primos", "contar primos hasta n (0 a 1.000.000)"),
    "7": ("esperar", "segundos (0 a 10), simula una tarea lenta"),
}


class Cliente:
    def __init__(self, host, puerto):
        self.sock = socket.create_connection((host, puerto))
        self._siguiente_id = 1
        self._cerrando = False
        self.pendientes = set()
        self._lock = threading.Lock()
        self._todas_listas = threading.Condition(self._lock)
        threading.Thread(target=self._escuchar, daemon=True).start()

    def enviar(self, tipo, datos):
        with self._lock:
            id_tarea = self._siguiente_id
            self._siguiente_id += 1
            self.pendientes.add(id_tarea)
        mensaje = {"id": id_tarea, "tipo": tipo, "datos": datos}
        self.sock.sendall((json.dumps(mensaje, ensure_ascii=False) + "\n").encode("utf-8"))
        print(f"  -> enviada tarea {id_tarea}: {tipo}({datos!r})")
        return id_tarea

    def _escuchar(self):
        try:
            with self.sock.makefile("r", encoding="utf-8") as entrada:
                for linea in entrada:
                    self._mostrar(json.loads(linea))
        except (OSError, ValueError):
            pass
        if not self._cerrando:
            print("\n[el servidor cerró la conexión]")
        with self._lock:
            self.pendientes.clear()
            self._todas_listas.notify_all()

    def _mostrar(self, r):
        if r.get("estado") == "ok":
            resultado = str(r["resultado"])
            if len(resultado) > 60:
                resultado = resultado[:57] + f"... ({len(resultado)} dígitos)"
            print(f"  <- tarea {r['id']} [{r['worker']}, {r['duracion_seg']} s]: {resultado}")
        else:
            print(f"  <- tarea {r.get('id')} ERROR: {r.get('error')}")
        with self._lock:
            self.pendientes.discard(r.get("id"))
            if not self.pendientes:
                self._todas_listas.notify_all()

    def esperar_resultados(self, timeout=30):
        with self._lock:
            self._todas_listas.wait_for(lambda: not self.pendientes, timeout=timeout)

    def cerrar(self):
        self._cerrando = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def convertir_datos(tipo, texto):
    if tipo == "suma":
        return [float(x) for x in texto.split()]
    if tipo in ("factorial", "primos"):
        return int(texto)
    if tipo == "esperar":
        return float(texto)
    return texto


def modo_demo(cliente):
    print("Enviando lote de tareas de ejemplo...\n")
    inicio = time.perf_counter()
    lote = [
        ("esperar", 2),
        ("esperar", 2),
        ("esperar", 2),
        ("mayusculas", "programación sobre redes"),
        ("suma", [10, 20, 30.5]),
        ("factorial", 25),
        ("primos", 100000),
        ("contar_palabras", "un sistema distribuido con sockets en Python"),
        ("eco", "hola servidor"),
        ("tarea_inexistente", None),
    ]
    for tipo, datos in lote:
        cliente.enviar(tipo, datos)
    print()
    cliente.esperar_resultados()
    print(f"\nLote completo en {time.perf_counter() - inicio:.2f} s "
          "(las tres esperas de 2 s corrieron en paralelo en distintos workers)")


def modo_interactivo(cliente):
    while True:
        print("\nTipos de tarea:")
        for clave, (tipo, ayuda) in TIPOS.items():
            print(f"  {clave}. {tipo:<16} {ayuda}")
        print("  0. salir")
        opcion = input("Opción: ").strip()
        if opcion == "0":
            cliente.esperar_resultados(timeout=10)
            return
        if opcion not in TIPOS:
            print("Opción inválida.")
            continue
        tipo, ayuda = TIPOS[opcion]
        texto = input(f"Datos ({ayuda}): ").strip()
        try:
            datos = convertir_datos(tipo, texto)
        except ValueError:
            print("Dato inválido para ese tipo de tarea.")
            continue
        cliente.enviar(tipo, datos)
        time.sleep(0.3)  # deja que llegue la respuesta antes de volver a mostrar el menú


def main():
    p = argparse.ArgumentParser(description="Cliente de tareas distribuidas (PFO 3)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=5000)
    p.add_argument("--demo", action="store_true", help="enviar un lote de tareas de ejemplo")
    args = p.parse_args()

    try:
        cliente = Cliente(args.host, args.puerto)
    except OSError:
        print(f"No se pudo conectar a {args.host}:{args.puerto}. ¿Está corriendo el servidor?")
        sys.exit(1)

    print(f"Conectado a {args.host}:{args.puerto}")
    try:
        modo_demo(cliente) if args.demo else modo_interactivo(cliente)
    except (KeyboardInterrupt, EOFError):
        print()
    finally:
        cliente.cerrar()


if __name__ == "__main__":
    main()
