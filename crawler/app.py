from __future__ import annotations

import streamlit as st

from crawler.config import get_settings
from crawler.services.youtube import (
    YouTubeServiceError,
    build_youtube_client,
    fetch_channel_videos,
    resolve_channel_id,
)
from crawler.utils import to_csv_bytes, to_json_bytes


st.set_page_config(page_title="Talk-index Crawler", page_icon="🎬", layout="wide")
st.title("🎬 YouTube動画取得ツール (MVP)")
st.caption("チャンネルIDまたはURLを入力して、動画情報を取得します。")

settings = get_settings()

if "logs" not in st.session_state:
    st.session_state.logs = []


def add_log(message: str) -> None:
    st.session_state.logs.append(message)


@st.cache_data(show_spinner=False)
def run_fetch(channel_input: str, max_results: int):
    local_logs: list[str] = []

    def logger(msg: str):
        local_logs.append(msg)

    youtube = build_youtube_client(settings.youtube_api_key)
    channel_id = resolve_channel_id(youtube, channel_input, logger)
    videos = fetch_channel_videos(youtube, channel_id, max_results, logger)
    return channel_id, [v.to_dict() for v in videos], local_logs


with st.sidebar:
    st.subheader("入力")
    channel_input = st.text_input(
        "チャンネルID または URL",
        placeholder="例: UCxxxx... / https://www.youtube.com/@...",
    )
    max_results = st.number_input(
        "取得件数上限",
        min_value=1,
        max_value=settings.max_allowed_results,
        value=settings.default_max_results,
        step=1,
    )
    run_button = st.button("取得する", type="primary", use_container_width=True)

if not settings.youtube_api_key:
    st.error("環境変数 YOUTUBE_API_KEY が未設定です。.env を確認してください。")

if run_button:
    st.session_state.logs = []

    if not channel_input.strip():
        st.warning("チャンネルIDまたはURLを入力してください。")
    elif not settings.youtube_api_key:
        pass
    else:
        try:
            with st.spinner("取得中..."):
                channel_id, rows, logs = run_fetch(channel_input.strip(), int(max_results))
            st.session_state.logs.extend(logs)
            add_log(f"取得完了: channel_id={channel_id}, 件数={len(rows)}")

            st.success(f"取得件数: {len(rows)}件")
            st.write(f"解決されたチャンネルID: `{channel_id}`")

            if rows:
                st.dataframe(rows, use_container_width=True)

                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        "JSONをダウンロード",
                        data=to_json_bytes(rows),
                        file_name="videos.json",
                        mime="application/json",
                        use_container_width=True,
                    )
                with col2:
                    st.download_button(
                        "CSVをダウンロード",
                        data=to_csv_bytes(rows),
                        file_name="videos.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
            else:
                st.info("動画が見つかりませんでした。")

        except YouTubeServiceError as exc:
            st.error(f"取得に失敗しました: {exc}")
        except Exception as exc:  # 予期しない例外
            st.error(f"予期しないエラーが発生しました: {exc}")

st.subheader("実行ログ")
if st.session_state.logs:
    st.code("\n".join(st.session_state.logs))
else:
    st.caption("まだログはありません。")
