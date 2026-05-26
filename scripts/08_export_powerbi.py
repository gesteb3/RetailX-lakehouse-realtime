import csv
import shutil
from pathlib import Path
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POWERBI_ROOT = PROJECT_ROOT / "data" / "powerbi"
FINAL_ROOT = POWERBI_ROOT / "final"


DATASETS = [
    "kpis_generales",
    "ventas_mensuales",
    "cliente_mayor_volumen",
    "top10_ventas",
    "promedio_venta_cliente",
    "pareto_80_clientes",
    "resumen_pareto",
    "ventas_categoria_canal",
    "web_kpis",
    "catalogo_gold",
    "rt_kpis_generales",
    "rt_kpis_por_zona",
    "rt_toneladas_por_hora",
    "rt_estado_flota",
    "rt_alertas_operativas",
    "rt_catalogo",
]


def find_part_csv(dataset_path: Path) -> Path:
    files = list(dataset_path.glob("part-*.csv"))

    if not files:
        raise FileNotFoundError(f"No se encontró archivo part-*.csv en {dataset_path}")

    return files[0]


def export_dataset(dataset_name: str) -> dict:
    source_path = POWERBI_ROOT / dataset_name
    target_path = FINAL_ROOT / f"{dataset_name}.csv"

    if not source_path.exists():
        return {
            "dataset": dataset_name,
            "status": "SKIPPED",
            "source": str(source_path),
            "target": str(target_path),
            "message": "Carpeta no encontrada",
        }

    source_file = find_part_csv(source_path)

    shutil.copy2(source_file, target_path)

    return {
        "dataset": dataset_name,
        "status": "EXPORTED",
        "source": str(source_file),
        "target": str(target_path),
        "message": "Exportado correctamente",
    }


def write_manifest(results) -> None:
    manifest_path = FINAL_ROOT / "powerbi_manifest.csv"

    with open(manifest_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "dataset",
                "status",
                "source",
                "target",
                "message",
                "generated_at_utc",
            ],
        )

        writer.writeheader()

        generated_at = datetime.now(timezone.utc).isoformat()

        for row in results:
            row["generated_at_utc"] = generated_at
            writer.writerow(row)


def main() -> None:
    FINAL_ROOT.mkdir(parents=True, exist_ok=True)

    results = []

    print("Exportando archivos finales para Power BI...")

    for dataset_name in DATASETS:
        try:
            result = export_dataset(dataset_name)
            results.append(result)
            print(f"{result['status']} - {dataset_name}")

        except Exception as error:
            results.append(
                {
                    "dataset": dataset_name,
                    "status": "ERROR",
                    "source": str(POWERBI_ROOT / dataset_name),
                    "target": str(FINAL_ROOT / f"{dataset_name}.csv"),
                    "message": str(error),
                }
            )
            print(f"ERROR - {dataset_name}: {error}")

    write_manifest(results)

    print("")
    print("Exportación finalizada.")
    print(f"Ruta final: {FINAL_ROOT}")


if __name__ == "__main__":
    main()