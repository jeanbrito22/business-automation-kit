def classificar_particao(num_arquivos, total_bytes, arquivos_individuais=None):
    if num_arquivos == 0:
        return {
            "status": "sem_dados",
            "total_mb": 0,
            "media_mb": 0,
            "maior_arquivo_mb": 0,
            "arquivos_menores_que_1mb": 0,
            "recomendar_compactacao": False,
            "sugestao_coalesce": 1
        }

    total_mb = total_bytes / (1024 * 1024)
    media_mb = (total_bytes / num_arquivos) / (1024 * 1024)

    arquivos_menores_que_1mb = 0
    maior_arquivo_mb = 0

    if arquivos_individuais:
        arquivos_menores_que_1mb = sum(1 for a in arquivos_individuais if a < 1024 * 1024)
        maior_arquivo_mb = max(arquivos_individuais) / (1024 * 1024)

    if num_arquivos == 1 and media_mb >= 128:
        status = "ideal"
    elif total_mb < 128 and num_arquivos <= 1:
        status = "ok_pequeno"
    elif num_arquivos > 100 and media_mb < 32:
        status = "small_file_crítico"
    elif num_arquivos > 10 and media_mb < 128:
        status = "small_file_grave"
    elif num_arquivos > 1 and media_mb < 128:
        status = "small_file_moderado"
    else:
        status = "ideal"

    recomendar = status.startswith("small_file")
    sugestao_coalesce = max(1, round(total_mb / 128))

    return {
        "status": status,
        "total_mb": round(total_mb, 2),
        "media_mb": round(media_mb, 2),
        "maior_arquivo_mb": round(maior_arquivo_mb, 2),
        "arquivos_menores_que_1mb": arquivos_menores_que_1mb,
        "recomendar_compactacao": recomendar,
        "sugestao_coalesce": sugestao_coalesce
    }
