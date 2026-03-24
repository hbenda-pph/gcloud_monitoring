"""
Script para comparar dos vistas de BigQuery

Compara:
- Cantidad de campos
- Tipos de campos
- Cantidad de registros

Uso:
    python compare_views.py "project1.dataset.view1" "project2.dataset.view2"
    
O con opciones:
    python compare_views.py "project1.dataset.view1" "project2.dataset.view2" --project-id PROJECT_ID
"""

import argparse
import sys
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest
from collections import OrderedDict
import pandas as pd
from typing import Dict, List, Tuple, Optional


def parse_view_reference(view_ref: str) -> Tuple[str, str, str]:
    """
    Parsea una referencia de vista en formato 'project.dataset.view'
    
    Args:
        view_ref: Referencia de la vista (ej: "project.dataset.view" o "dataset.view")
        
    Returns:
        Tuple (project_id, dataset_id, view_id)
    """
    parts = view_ref.split('.')
    
    if len(parts) == 3:
        return tuple(parts)
    elif len(parts) == 2:
        # Si solo hay 2 partes, asumir que falta el project_id
        # Se usará el project_id del cliente BigQuery
        return (None, parts[0], parts[1])
    else:
        raise ValueError(f"Formato de vista inválido: {view_ref}. Use 'project.dataset.view' o 'dataset.view'")


def get_view_schema(client: bigquery.Client, project_id: str, dataset_id: str, view_id: str) -> List[bigquery.SchemaField]:
    """
    Obtiene el esquema (campos y tipos) de una vista.
    
    Args:
        client: Cliente BigQuery
        project_id: ID del proyecto
        dataset_id: ID del dataset
        view_id: ID de la vista
        
    Returns:
        Lista de SchemaField
    """
    # Construir referencia completa
    if project_id:
        view_ref = f"{project_id}.{dataset_id}.{view_id}"
    else:
        view_ref = f"{dataset_id}.{view_id}"
    
    try:
        # Obtener la vista
        table = client.get_table(view_ref)
        return list(table.schema)
    except NotFound:
        raise NotFound(f"Vista no encontrada: {view_ref}")
    except Exception as e:
        raise Exception(f"Error obteniendo esquema de {view_ref}: {str(e)}")


def get_view_row_count(client: bigquery.Client, project_id: str, dataset_id: str, view_id: str) -> int:
    """
    Obtiene la cantidad de registros de una vista.
    
    Args:
        client: Cliente BigQuery
        project_id: ID del proyecto
        dataset_id: ID del dataset
        view_id: ID de la vista
        
    Returns:
        Cantidad de registros
    """
    # Construir referencia completa
    if project_id:
        view_ref = f"{project_id}.{dataset_id}.{view_id}"
    else:
        view_ref = f"{dataset_id}.{view_id}"
    
    try:
        query = f"SELECT COUNT(*) as row_count FROM `{view_ref}`"
        result = client.query(query).to_dataframe()
        return int(result.iloc[0]['row_count'])
    except Exception as e:
        raise Exception(f"Error obteniendo cantidad de registros de {view_ref}: {str(e)}")


def flatten_schema(schema_fields: List[bigquery.SchemaField], prefix: str = "") -> Dict[str, str]:
    """
    Aplana el esquema recursivamente para manejar campos anidados (STRUCT).
    
    Args:
        schema_fields: Lista de SchemaField
        prefix: Prefijo para campos anidados
        
    Returns:
        Diccionario {nombre_campo: tipo}
    """
    fields = {}
    
    for field in schema_fields:
        field_name = f"{prefix}.{field.name}" if prefix else field.name
        
        if field.field_type == 'RECORD':
            # Campo anidado (STRUCT), procesar recursivamente
            nested_fields = flatten_schema(field.fields, field_name)
            fields.update(nested_fields)
        else:
            # Campo simple
            mode = f" ({field.mode})" if field.mode != 'NULLABLE' else ""
            fields[field_name] = f"{field.field_type}{mode}"
    
    return fields


def compare_schemas(schema1: Dict[str, str], schema2: Dict[str, str]) -> Dict:
    """
    Compara dos esquemas y retorna un reporte de diferencias.
    
    Args:
        schema1: Esquema de la primera vista (dict {campo: tipo})
        schema2: Esquema de la segunda vista (dict {campo: tipo})
        view1_name: Nombre de la primera vista
        view2_name: Nombre de la segunda vista
        
    Returns:
        Diccionario con el reporte de comparación
    """
    fields1 = set(schema1.keys())
    fields2 = set(schema2.keys())
    
    # Campos comunes
    common_fields = fields1 & fields2
    
    # Campos solo en vista 1
    only_in_view1 = fields1 - fields2
    
    # Campos solo en vista 2
    only_in_view2 = fields2 - fields1
    
    # Campos con tipos diferentes
    type_differences = []
    for field in common_fields:
        if schema1[field] != schema2[field]:
            type_differences.append({
                'campo': field,
                'tipo_vista1': schema1[field],
                'tipo_vista2': schema2[field]
            })
    
    return {
        'total_campos_vista1': len(fields1),
        'total_campos_vista2': len(fields2),
        'campos_comunes': len(common_fields),
        'solo_en_vista1': list(only_in_view1),
        'solo_en_vista2': list(only_in_view2),
        'diferencias_tipo': type_differences,
        'esquema_vista1': schema1,
        'esquema_vista2': schema2
    }


def print_comparison_report(view1_ref: str, view2_ref: str, 
                           schema_comparison: Dict, row_count1: int, row_count2: int):
    """
    Imprime un reporte detallado de la comparación.
    
    Args:
        view1_ref: Referencia de la primera vista
        view2_ref: Referencia de la segunda vista
        schema_comparison: Resultado de compare_schemas
        row_count1: Cantidad de registros de la primera vista
        row_count2: Cantidad de registros de la segunda vista
    """
    print("=" * 80)
    print("COMPARACIÓN DE VISTAS")
    print("=" * 80)
    print(f"\n📊 Vista 1: {view1_ref}")
    print(f"📊 Vista 2: {view2_ref}")
    print("\n" + "-" * 80)
    
    # Comparación de cantidad de campos
    print("\n🔢 CANTIDAD DE CAMPOS")
    print("-" * 80)
    print(f"Vista 1: {schema_comparison['total_campos_vista1']} campos")
    print(f"Vista 2: {schema_comparison['total_campos_vista2']} campos")
    print(f"Campos comunes: {schema_comparison['campos_comunes']} campos")
    
    diff = schema_comparison['total_campos_vista1'] - schema_comparison['total_campos_vista2']
    if diff > 0:
        print(f"⚠️  Vista 1 tiene {diff} campo(s) más que Vista 2")
    elif diff < 0:
        print(f"⚠️  Vista 2 tiene {abs(diff)} campo(s) más que Vista 1")
    else:
        print("✅ Ambas vistas tienen la misma cantidad de campos")
    
    # Comparación de cantidad de registros
    print("\n📈 CANTIDAD DE REGISTROS")
    print("-" * 80)
    print(f"Vista 1: {row_count1:,} registros")
    print(f"Vista 2: {row_count2:,} registros")
    
    diff_rows = row_count1 - row_count2
    if diff_rows > 0:
        print(f"⚠️  Vista 1 tiene {diff_rows:,} registro(s) más que Vista 2")
        pct_diff = (diff_rows / row_count2 * 100) if row_count2 > 0 else 0
        print(f"   Diferencia: {pct_diff:.2f}%")
    elif diff_rows < 0:
        print(f"⚠️  Vista 2 tiene {abs(diff_rows):,} registro(s) más que Vista 1")
        pct_diff = (abs(diff_rows) / row_count1 * 100) if row_count1 > 0 else 0
        print(f"   Diferencia: {pct_diff:.2f}%")
    else:
        print("✅ Ambas vistas tienen la misma cantidad de registros")
    
    # Campos solo en Vista 1
    if schema_comparison['solo_en_vista1']:
        print("\n🔍 CAMPOS SOLO EN VISTA 1")
        print("-" * 80)
        for field in sorted(schema_comparison['solo_en_vista1']):
            field_type = schema_comparison['esquema_vista1'][field]
            print(f"  • {field} ({field_type})")
    
    # Campos solo en Vista 2
    if schema_comparison['solo_en_vista2']:
        print("\n🔍 CAMPOS SOLO EN VISTA 2")
        print("-" * 80)
        for field in sorted(schema_comparison['solo_en_vista2']):
            field_type = schema_comparison['esquema_vista2'][field]
            print(f"  • {field} ({field_type})")
    
    # Diferencias de tipo
    if schema_comparison['diferencias_tipo']:
        print("\n⚠️  CAMPOS CON TIPOS DIFERENTES")
        print("-" * 80)
        for diff in schema_comparison['diferencias_tipo']:
            print(f"  Campo: {diff['campo']}")
            print(f"    Vista 1: {diff['tipo_vista1']}")
            print(f"    Vista 2: {diff['tipo_vista2']}")
            print()
    
    # Resumen
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    
    all_match = (
        schema_comparison['total_campos_vista1'] == schema_comparison['total_campos_vista2'] and
        len(schema_comparison['solo_en_vista1']) == 0 and
        len(schema_comparison['solo_en_vista2']) == 0 and
        len(schema_comparison['diferencias_tipo']) == 0 and
        row_count1 == row_count2
    )
    
    if all_match:
        print("✅ Las vistas son idénticas en estructura y cantidad de registros")
    else:
        print("⚠️  Se encontraron diferencias entre las vistas")
        print(f"   - Campos diferentes: {len(schema_comparison['solo_en_vista1']) + len(schema_comparison['solo_en_vista2'])}")
        print(f"   - Tipos diferentes: {len(schema_comparison['diferencias_tipo'])}")
        print(f"   - Diferencia de registros: {abs(diff_rows):,}")
    
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Compara dos vistas de BigQuery en cantidad y tipos de campos, y cantidad de registros',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Comparar dos vistas completas
  python compare_views.py "project1.dataset.view1" "project2.dataset.view2"
  
  # Comparar vistas en el mismo proyecto (solo dataset.view)
  python compare_views.py "dataset.view1" "dataset.view2" --project-id my-project
  
  # Especificar proyecto explícitamente
  python compare_views.py "project1.dataset.view1" "project2.dataset.view2" --project-id my-project
        """
    )
    
    parser.add_argument('view1', help='Referencia de la primera vista (formato: project.dataset.view o dataset.view)')
    parser.add_argument('view2', help='Referencia de la segunda vista (formato: project.dataset.view o dataset.view)')
    parser.add_argument('--project-id', help='ID del proyecto BigQuery (opcional, se detecta automáticamente si no se especifica)')
    parser.add_argument('--output', help='Archivo de salida para guardar el reporte (formato CSV o JSON)')
    
    args = parser.parse_args()
    
    try:
        # Parsear referencias de vistas
        proj1, ds1, v1 = parse_view_reference(args.view1)
        proj2, ds2, v2 = parse_view_reference(args.view2)
        
        # Determinar project_id a usar
        if args.project_id:
            project_id = args.project_id
        elif proj1:
            project_id = proj1
        elif proj2:
            project_id = proj2
        else:
            # Intentar detectar desde el cliente BigQuery
            try:
                client_temp = bigquery.Client()
                project_id = client_temp.project
                if not project_id:
                    raise ValueError("No se pudo determinar el project_id. Especifíquelo con --project-id")
            except Exception as e:
                raise ValueError(f"No se pudo determinar el project_id: {str(e)}. Especifíquelo con --project-id")
        
        # Usar project_id explícito si no está en las referencias
        if not proj1:
            proj1 = project_id
        if not proj2:
            proj2 = project_id
        
        print(f"🔧 Usando project_id: {project_id}")
        print(f"📊 Vista 1: {proj1}.{ds1}.{v1}")
        print(f"📊 Vista 2: {proj2}.{ds2}.{v2}")
        print("\n⏳ Obteniendo información de las vistas...\n")
        
        # Crear cliente BigQuery
        client = bigquery.Client(project=project_id)
        
        # Obtener esquemas
        print("📋 Obteniendo esquemas...")
        schema1_fields = get_view_schema(client, proj1, ds1, v1)
        schema2_fields = get_view_schema(client, proj2, ds2, v2)
        
        # Aplanar esquemas
        schema1_flat = flatten_schema(schema1_fields)
        schema2_flat = flatten_schema(schema2_fields)
        
        # Obtener cantidad de registros
        print("📈 Obteniendo cantidad de registros...")
        row_count1 = get_view_row_count(client, proj1, ds1, v1)
        row_count2 = get_view_row_count(client, proj2, ds2, v2)
        
        # Comparar esquemas
        print("🔍 Comparando esquemas...\n")
        view1_ref = f"{proj1}.{ds1}.{v1}"
        view2_ref = f"{proj2}.{ds2}.{v2}"
        schema_comparison = compare_schemas(schema1_flat, schema2_flat)
        
        # Imprimir reporte
        print_comparison_report(view1_ref, view2_ref, schema_comparison, row_count1, row_count2)
        
        # Guardar a archivo si se especifica
        if args.output:
            print(f"\n💾 Guardando reporte en {args.output}...")
            if args.output.endswith('.csv'):
                # Crear DataFrame con el resumen
                summary_data = {
                    'Métrica': [
                        'Total campos Vista 1',
                        'Total campos Vista 2',
                        'Campos comunes',
                        'Campos solo en Vista 1',
                        'Campos solo en Vista 2',
                        'Campos con tipos diferentes',
                        'Registros Vista 1',
                        'Registros Vista 2',
                        'Diferencia de registros'
                    ],
                    'Valor': [
                        schema_comparison['total_campos_vista1'],
                        schema_comparison['total_campos_vista2'],
                        schema_comparison['campos_comunes'],
                        len(schema_comparison['solo_en_vista1']),
                        len(schema_comparison['solo_en_vista2']),
                        len(schema_comparison['diferencias_tipo']),
                        row_count1,
                        row_count2,
                        abs(row_count1 - row_count2)
                    ]
                }
                df = pd.DataFrame(summary_data)
                df.to_csv(args.output, index=False)
                print(f"✅ Reporte guardado en {args.output}")
            elif args.output.endswith('.json'):
                import json
                report = {
                    'vista1': view1_ref,
                    'vista2': view2_ref,
                    'resumen': {
                        'total_campos_vista1': schema_comparison['total_campos_vista1'],
                        'total_campos_vista2': schema_comparison['total_campos_vista2'],
                        'campos_comunes': schema_comparison['campos_comunes'],
                        'registros_vista1': row_count1,
                        'registros_vista2': row_count2,
                        'diferencia_registros': abs(row_count1 - row_count2)
                    },
                    'campos_solo_vista1': schema_comparison['solo_en_vista1'],
                    'campos_solo_vista2': schema_comparison['solo_en_vista2'],
                    'diferencias_tipo': schema_comparison['diferencias_tipo']
                }
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
                print(f"✅ Reporte guardado en {args.output}")
            else:
                print(f"⚠️  Formato de archivo no soportado: {args.output}. Use .csv o .json")
        
    except NotFound as e:
        print(f"❌ Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except BadRequest as e:
        print(f"❌ Error en la consulta: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
