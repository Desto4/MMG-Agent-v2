if "drafts" not in st.session_state:
    st.session_state.drafts = []

if "editing_index" not in st.session_state:
    st.session_state.editing_index = None

def render_editable_email_card(draft, index):
    is_editing = st.session_state.editing_index == index

    st.markdown(f"""
    <div style="
        border:1px solid #e6e6e6;
        border-radius:12px;
        padding:20px;
        margin-bottom:20px;
        background:white;
        box-shadow:0 2px 6px rgba(0,0,0,0.05);
    ">
    """, unsafe_allow_html=True)

    # --- HEADER ---
    st.markdown(f"**To:** {draft.get('email','N/A')}")

    if is_editing:
        # --- EDIT MODE ---
        subject = st.text_input(
            "Subject",
            value=draft.get("subject_line", ""),
            key=f"subject_{index}"
        )

        body = st.text_area(
            "Email Body",
            value=draft.get("email_body", ""),
            height=200,
            key=f"body_{index}"
        )

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("💾 Save", key=f"save_{index}"):
                st.session_state.drafts[index]["subject_line"] = subject
                st.session_state.drafts[index]["email_body"] = body
                st.session_state.editing_index = None
                st.rerun()

        with col2:
            if st.button("❌ Cancel", key=f"cancel_{index}"):
                st.session_state.editing_index = None
                st.rerun()

        with col3:
            if st.button("🔁 Regenerate", key=f"regen_{index}"):
                # Hook your LLM here
                st.session_state.drafts[index]["email_body"] = "Regenerated version..."
                st.rerun()

    else:
        # --- VIEW MODE ---
        st.markdown(f"**Subject:** {draft.get('subject_line','')}")

        st.markdown(f"""
        <div style="
            background:#f9f9f9;
            padding:15px;
            border-radius:8px;
            white-space:pre-wrap;
            font-size:14px;
            line-height:1.5;
        ">
        {draft.get("email_body","")}
        </div>
        """, unsafe_allow_html=True)

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("✏️ Edit", key=f"edit_{index}"):
                st.session_state.editing_index = index
                st.rerun()

        with col2:
            st.button("📋 Copy", key=f"copy_{index}")

        with col3:
            st.button("🚀 Send", key=f"send_{index}")

    st.markdown("</div>", unsafe_allow_html=True)


def render_all_emails():
    for i, draft in enumerate(st.session_state.drafts):
        render_editable_email_card(draft, i)


st.title("📬 AI Outreach Workspace")

# Example data (replace with your agent output)
st.session_state.drafts = [
    {
        "name": "John",
        "email": "john@salon.com",
        "subject_line": "Quick idea for your salon",
        "email_body": "Hi John,\n\nI noticed...\n\nBest,\nYour Name"
    }
]

render_all_emails()



