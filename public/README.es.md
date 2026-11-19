# Bots de trading para Polymarket | Sniper de 1¢ y 99¢ · Copy trading · Arbitraje con Kalshi · Market making

**Idiomas:** [English](../README.md) · [中文](README.zh-CN.md) · [Русский](README.ru.md) · [Español](README.es.md)

> **Un repositorio, cinco bots para Polymarket, desarrollados y operados por un equipo que realiza market making en Polymarket con fondos reales.**
> **Núcleos de código abierto • Simulación primero • Ejecución real en cadena • Python y Rust**

> **¿Necesitas ayuda o una implementación para producción?**
> 📱 **Telegram**: [t.me/dexoryn](https://t.me/dexoryn) | 🎮 **Discord**: `dexoryn_` | 🐦 **X**: [@Dexoryn](https://x.com/Dexoryn)

---

## 🤖 Los bots

| Bot | Estado | Qué hace | Carpeta |
|-----|--------|----------|---------|
| **Bot de copy trading** | ✅ Probado con fondos reales, código abierto | Replica las operaciones de otras carteras en tiempo real: múltiples carteras, mercados de predicción y perpetuos, panel web | [`copy-trading-bot/`](../copy-trading-bot/) |
| **Bot de arbitraje Polymarket ↔ Kalshi** | 🚧 En desarrollo | Identifica el mismo evento en ambas plataformas y detecta diferencias de precio después de comisiones | [`kalshi-arbitrage-bot/`](../kalshi-arbitrage-bot/) |
| **Bot de market making** | 🚧 Versión de demostración prevista | Publica cotizaciones de compra y venta con límites de inventario: implementación de referencia de nuestra operativa de market making | [`market-maker-bot/`](../market-maker-bot/) |
| **Bot sniper de 1¢** | ✅ Probado con fondos reales, rentable, código abierto | Compra el resultado menos probable a ~1¢ en los mercados de BTC de 5 minutos justo cuando Binance gira a su favor: arriesga 1¢ para ganar 99¢ | [`1c-99c-sniper-bot/`](../1c-99c-sniper-bot/) |
| **Bot sniper de 99¢** | ✅ Probado con fondos reales, rentable, código abierto | Compra al ganador casi seguro a 97–99¢ en los últimos minutos antes de la resolución | [`1c-99c-sniper-bot/`](../1c-99c-sniper-bot/) |

**Empieza aquí:** el bot de copy trading y los dos snipers están probados con fondos reales y son rentables. Los snipers de 1¢ y 99¢ comparten un mismo motor en Rust en [`1c-99c-sniper-bot/`](../1c-99c-sniper-bot/); elige uno con `SNIPER_MODE=1c` o `99c` en `.env`. Los demás se publicarán uno a uno. Sigue el repositorio o a [@Dexoryn](https://x.com/Dexoryn) para conocer cada lanzamiento.

---

## 🎥 Pruebas de ejecución real (bot de copy trading)

Operaciones reales en cadena realizadas con el bot de este repositorio, no simulaciones.

https://github.com/user-attachments/assets/e62d908d-4dbd-4c7f-93a2-20879baed8da

- **+$80 en ~15 minutos**, con el bot funcionando sin supervisión
- Una segunda sesión al día siguiente añadió **+$230**: el mismo código, en otra ejecución

<p align="center">
  <img src="Realtradehistory/securebet.jpg" alt="Ganancias y pérdidas del copy trading: comparación entre la cartera del bot y la cartera objetivo securebet" width="100%"/>
</p>

La cartera del bot (izquierda) y la del trader copiado (derecha) muestran **la misma forma de la curva de capital** durante el día. Los vídeos completos, las experiencias y la configuración están en [`copy-trading-bot/README.md`](../copy-trading-bot/README.md).

---

## 🚀 Inicio rápido (bot de copy trading)

```bash
git clone https://github.com/dexorynlabs/polymarket-trading-bots.git
cd polymarket-trading-bots/copy-trading-bot

pip install -r requirements.txt
cp config.yaml.example config.yaml   # conserva mode: dry_run en la primera ejecución

python -m app.main
# panel → http://127.0.0.1:8787
```

Cada carpeta de bot es independiente y tiene su propio README, archivo de dependencias y configuración. Ningún bot opera con fondos reales hasta que actives explícitamente el modo `real` o en vivo.

---

## 🛡 Seguridad

- Todos los bots usan **simulación / dry-run por defecto**; el modo real requiere activación explícita
- Usa una cartera dedicada con saldo limitado
- Nunca incluyas `config.yaml`, claves ni credenciales de API en tus commits
- Los resultados pasados no garantizan resultados futuros

---

## 👤 Autor y contacto

**Dexoryn Labs**: automatización de trading en Polymarket. Operamos bots de market making y ejecución en Polymarket a diario; las versiones de código abierto de este repositorio contienen los núcleos que hemos decidido publicar.

📱 **Telegram:** [@dexoryn](https://t.me/dexoryn) &nbsp; · &nbsp; 🐦 **X:** [@Dexoryn](https://x.com/Dexoryn) &nbsp; · &nbsp; 🎮 **Discord:** `dexoryn_`

<p align="center">
  <img src="dexoryn_tg.jpg" alt="Código QR de Telegram - @dexoryn" height="260"/>
  &nbsp;&nbsp;
  <img src="dexoryn_wechat.png" alt="Código QR de WeChat - DexorynWe" height="260"/>
</p>

---

## Contribuciones

Consulta [CONTRIBUTING.md](../.github/CONTRIBUTING.md). Comunidad: [Código de conducta](../.github/CODE_OF_CONDUCT.md) · [Seguridad](../.github/SECURITY.md) · [Licencia MIT](../LICENSE)

## Aviso legal

Operar en Polymarket y Kalshi implica un **riesgo considerable de pérdida**. Dexoryn no se hace responsable de las pérdidas derivadas del uso de este software. La seguridad de tu cartera, la configuración y el capital en riesgo son exclusivamente tu responsabilidad.

**Opera únicamente con fondos que puedas permitirte perder.**
