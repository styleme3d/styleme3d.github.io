import cv2
import os
import multiprocessing
from tqdm import tqdm
from pathlib import Path
import functools

def process_video(video_path_str, output_dir_str):
    """
    处理单个视频文件：提取帧，BGR 转 RGB，并保存为新的 MP4 文件。

    Args:
        video_path_str (str): 输入视频文件的路径字符串。
        output_dir_str (str): 输出目录的路径字符串。
    """
    try:
        video_path = Path(video_path_str)
        output_dir = Path(output_dir_str)
        # 构建输出文件名，添加 'processed_' 前缀并确保是 .mp4 格式
        output_filename = f"{video_path.stem}.mp4"
        output_path = output_dir / output_filename

        # 确保输出目录存在
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- 读取视频 ---
        cap = cv2.VideoCapture(str(video_path))
        if not cap.isOpened():
            print(f"警告：无法打开视频文件 {video_path}，跳过处理。")
            return # 返回而不是更新进度条，因为没有实际处理

        # 获取视频属性
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        # 确保 FPS 有效，否则设置默认值
        if fps <= 0:
            print(f"警告：视频 {video_path} 的 FPS 无效 ({fps})，将使用默认值 30。")
            fps = 30.0
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        # --- 设置视频写入器 ---
        # 使用 'mp4v' 编码器来创建 MP4 文件
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(str(output_path), fourcc, fps, (width, height))

        if not out.isOpened():
            print(f"错误：无法创建输出视频文件 {output_path}。")
            cap.release()
            return

        # --- 逐帧处理 ---
        processed_frames = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                # 检查是否是因为文件末尾或读取错误
                if processed_frames < frame_count:
                     # 可能只是最后一帧读取失败或者帧数计算不精确
                     pass # 继续执行后续清理操作
                break # 正常结束或发生错误

            # 颜色空间转换: BGR -> RGB
            try:
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            except cv2.error as e:
                print(f"错误：在处理 {video_path} 的第 {processed_frames + 1} 帧时进行颜色转换失败: {e}")
                # 可以选择跳过此帧或中止处理
                continue # 跳过写入此帧

            # 写入转换后的帧
            out.write(rgb_frame)
            processed_frames += 1

        # --- 清理 ---
        cap.release()
        out.release()
        # print(f"完成处理: {video_path.name} -> {output_path}")

    except Exception as e:
        print(f"处理 {video_path_str} 时发生意外错误: {e}")
        # 确保在异常情况下也释放资源
        if 'cap' in locals() and cap.isOpened():
            cap.release()
        if 'out' in locals() and out.isOpened():
            out.release()


if __name__ == '__main__':
    # --- 配置 ---
    source_dir = '.'  # 包含视频的源目录 (当前目录)
    output_dir = 'processed_videos' # 处理后视频的输出目录
    # 添加更多你想要处理的视频格式
    video_extensions = ['.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv']
    # 使用 CPU 核心数作为进程数，可以根据需要调整
    num_processes =12 # 留一半核心给系统或其他任务
    # -------------

    # 获取脚本所在目录的绝对路径
    script_dir = Path(__file__).parent.resolve()
    source_path = (script_dir / source_dir).resolve()
    output_path = (script_dir / output_dir).resolve()

    print(f"视频源目录: {source_path}")
    print(f"输出目录: {output_path}")
    print(f"将使用 {num_processes} 个进程进行处理")

    # 查找源目录中的视频文件
    video_files = [
        f for f in source_path.iterdir()
        if f.is_file() and f.suffix.lower() in video_extensions
    ]

    if not video_files:
        print("在源目录中未找到任何支持的视频文件。")
        exit()

    print(f"找到 {len(video_files)} 个视频文件待处理:")
    for vf in video_files:
        print(f" - {vf.name}")

    # 创建输出目录 (如果不存在)
    output_path.mkdir(parents=True, exist_ok=True)

    # 设置并启动多进程处理
    # 创建总进度条 (不再需要 Manager)
    pbar = tqdm(total=len(video_files), desc="整体进度", unit="视频")

    # 创建偏函数，固定 output_dir 参数
    process_func = functools.partial(process_video, output_dir_str=str(output_path))

    # 创建进程池
    with multiprocessing.Pool(processes=num_processes) as pool:
        # 使用 imap_unordered 来异步执行任务并在主进程中更新进度条
        # imap_unordered 按完成顺序返回结果，效率更高
        for _ in pool.imap_unordered(process_func, [str(vf) for vf in video_files]):
            pbar.update(1) # 每完成一个任务，主进程更新进度条

    # 关闭进度条
    pbar.close()

    print(f"所有视频处理完成！")
    print(f"处理后的视频已保存至: {output_path}") 