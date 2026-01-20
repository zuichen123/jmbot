def merge_images(folder_path, limit=None, delete_org_file=True, suffix='.webp'):
    import os
    from PIL import Image
    # 获取文件夹名称
    folder_name = os.path.basename(folder_path)

    # 获取所有webp图片路径

    webp_files = [f for f in os.listdir(folder_path) if f.endswith(suffix)]

    # 如果没有webp图片则报错
    if not webp_files:
        raise ValueError(f'文件夹中没有{suffix}图片')

    # 根据limit参数拆分webp_files列表
    if limit:
        webp_list = [webp_files[i:i + limit]
                     for i in range(0, len(webp_files), limit)]
    else:
        webp_list = [webp_files]

    # 打印图片数量、总长度和总宽度
    num_images = len(webp_files)
    total_width = 0
    total_height = 0
    for file_name in webp_files:
        img_path = os.path.join(folder_path, file_name)
        with Image.open(img_path) as im:
            total_width = max(total_width, im.width)
            total_height += im.height
    print(f'共有{num_images}张图片，总长度为{total_width}，总高度为{total_height}')

    # 拼接图片并保存
    for i, webp_group in enumerate(webp_list):
        images = []
        for file_name in webp_group:
            img_path = os.path.join(folder_path, file_name)
            with Image.open(img_path) as im:
                images.append(im)

        # 对所有图片进行纵向拼接
        result = Image.new('RGB', (images[0].width, sum(
            img.height for img in images)), (255, 255, 255))
        y_offset = 0
        for img in images:
            result.paste(img, (0, y_offset))
            y_offset += img.size[1]

        # 保存结果图片
        if len(webp_list) > 1:
            img_name = f'{folder_name}_{i + 1}{suffix}'
        else:
            img_name = f'{folder_name}{suffix}'

        save_path = os.path.join(folder_path, img_name)
        result.save(save_path)
        print(f'保存: {save_path}')

    if delete_org_file is True:
        for f in webp_files:
            os.remove(os.path.join(folder_path, f))
