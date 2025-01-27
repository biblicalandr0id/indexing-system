import tkinter as tk
from tkinter import ttk, messagebox
import customtkinter as ctk
from PIL import Image, ImageTk
import json
import threading
import queue
import time
from typing import Dict, Any, Optional
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import numpy as np
from ttkthemes import ThemedTk

class ModernIndexingUI:
    def __init__(self):
        # Initialize the main window with a modern theme
        self.root = ThemedTk(theme="arc")
        self.root.title("Indexing System Control Center")
        self.root.geometry("1200x800")
        
        # Configure colors and styles
        self.colors = {
            'primary': '#2962ff',
            'secondary': '#455a64',
            'success': '#4caf50',
            'warning': '#ff9800',
            'error': '#f44336',
            'background': '#f5f5f5',
            'surface': '#ffffff'
        }
        
        # Create styles
        self.style = ttk.Style()
        self._setup_styles()
        
        # Initialize components
        self.security_manager = None
        self.tracing_manager = None
        self.ops_manager = None
        
        # Setup UI components
        self._create_main_layout()
        self._setup_navigation()
        self._setup_metrics_display()
        self._setup_search_interface()
        self._setup_security_controls()
        
        # Start metrics update thread
        self.metrics_queue = queue.Queue()
        self.update_thread = threading.Thread(target=self._update_metrics_thread, daemon=True)
        self.update_thread.start()

    def _setup_styles(self):
        """Configure custom styles for widgets"""
        self.style.configure(
            "Card.TFrame",
            background=self.colors['surface'],
            relief="raised",
            borderwidth=1
        )
        
        self.style.configure(
            "Primary.TButton",
            background=self.colors['primary'],
            foreground="white",
            padding=(20, 10),
            font=('Helvetica', 10)
        )
        
        self.style.configure(
            "Header.TLabel",
            font=('Helvetica', 16, 'bold'),
            foreground=self.colors['secondary']
        )

    def _create_main_layout(self):
        """Create the main application layout"""
        # Main container
        self.main_container = ttk.Frame(self.root, padding="10")
        self.main_container.grid(row=0, column=0, sticky="nsew")
        
        # Configure grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        self.main_container.columnconfigure(1, weight=1)
        self.main_container.rowconfigure(0, weight=1)
        
        # Create sidebar
        self.sidebar = ttk.Frame(
            self.main_container,
            style="Card.TFrame",
            padding="10"
        )
        self.sidebar.grid(row=0, column=0, sticky="ns", padx=(0, 10))
        
        # Create main content area
        self.content = ttk.Frame(
            self.main_container,
            style="Card.TFrame",
            padding="20"
        )
        self.content.grid(row=0, column=1, sticky="nsew")

    def _setup_navigation(self):
        """Create navigation menu"""
        nav_items = [
            ("Dashboard", self._show_dashboard),
            ("Search", self._show_search),
            ("Security", self._show_security),
            ("Settings", self._show_settings)
        ]
        
        for i, (text, command) in enumerate(nav_items):
            btn = ttk.Button(
                self.sidebar,
                text=text,
                style="Primary.TButton",
                command=command
            )
            btn.grid(row=i, column=0, pady=5, sticky="ew")

    def _setup_metrics_display(self):
        """Create metrics dashboard"""
        self.metrics_frame = ttk.Frame(self.content)
        
        # Create graphs
        self.fig = Figure(figsize=(8, 4), dpi=100)
        self.fig.patch.set_facecolor(self.colors['surface'])
        
        # CPU Usage graph
        self.cpu_ax = self.fig.add_subplot(221)
        self.cpu_line, = self.cpu_ax.plot([], [], label='CPU Usage')
        self.cpu_ax.set_title('CPU Usage')
        
        # Memory Usage graph
        self.mem_ax = self.fig.add_subplot(222)
        self.mem_line, = self.mem_ax.plot([], [], label='Memory Usage')
        self.mem_ax.set_title('Memory Usage')
        
        # Query Latency graph
        self.latency_ax = self.fig.add_subplot(223)
        self.latency_line, = self.latency_ax.plot([], [], label='Query Latency')
        self.latency_ax.set_title('Query Latency')
        
        # Auth Failures graph
        self.auth_ax = self.fig.add_subplot(224)
        self.auth_line, = self.auth_ax.plot([], [], label='Auth Failures')
        self.auth_ax.set_title('Auth Failures')
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.metrics_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def _setup_search_interface(self):
        """Create search interface"""
        self.search_frame = ttk.Frame(self.content)
        
        # Search input
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(
            self.search_frame,
            textvariable=self.search_var,
            font=('Helvetica', 12)
        )
        search_entry.pack(fill=tk.X, pady=10)
        
        # Search button
        search_btn = ttk.Button(
            self.search_frame,
            text="Search",
            style="Primary.TButton",
            command=self._perform_search
        )
        search_btn.pack(pady=5)
        
        # Results area
        self.results_text = tk.Text(
            self.search_frame,
            height=10,
            font=('Helvetica', 10),
            wrap=tk.WORD
        )
        self.results_text.pack(fill=tk.BOTH, expand=True, pady=10)

    def _setup_security_controls(self):
        """Create security management interface"""
        self.security_frame = ttk.Frame(self.content)
        
        # User management
        users_label = ttk.Label(
            self.security_frame,
            text="User Management",
            style="Header.TLabel"
        )
        users_label.pack(pady=10)
        
        # User list
        self.users_tree = ttk.Treeview(
            self.security_frame,
            columns=('Username', 'Role', 'Status'),
            show='headings'
        )
        self.users_tree.heading('Username', text='Username')
        self.users_tree.heading('Role', text='Role')
        self.users_tree.heading('Status', text='Status')
        self.users_tree.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Security controls
        controls_frame = ttk.Frame(self.security_frame)
        controls_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(
            controls_frame,
            text="Add User",
            style="Primary.TButton",
            command=self._add_user_dialog
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            controls_frame,
            text="Remove User",
            style="Primary.TButton",
            command=self._remove_user
        ).pack(side=tk.LEFT, padx=5)

    def _show_dashboard(self):
        """Switch to dashboard view"""
        self._clear_content()
        self.metrics_frame.grid(row=0, column=0, sticky="nsew")

    def _show_search(self):
        """Switch to search view"""
        self._clear_content()
        self.search_frame.grid(row=0, column=0, sticky="nsew")

    def _show_security(self):
        """Switch to security view"""
        self._clear_content()
        self.security_frame.grid(row=0, column=0, sticky="nsew")

    def _show_settings(self):
        """Switch to settings view"""
        self._clear_content()
        # Implement settings view

    def _clear_content(self):
        """Clear current content"""
        for widget in self.content.winfo_children():
            widget.grid_forget()

    def _perform_search(self):
        """Execute search operation"""
        query = self.search_var.get()
        try:
            # Validate input
            InputValidator.validate_query({"query": query})
            
            # Perform search with security check
            @secure_endpoint(required_permission="search")
            def do_search(query):
                results = self._search_documents(query)
                self.results_text.delete(1.0, tk.END)
                self.results_text.insert(tk.END, json.dumps(results, indent=2))
            
            do_search(query)
            
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _update_metrics_thread(self):
        """Background thread for updating metrics"""
        while True:
            try:
                # Simulate metrics updates
                cpu = np.random.uniform(0, 100)
                mem = np.random.uniform(0, 16)  # GB
                latency = np.random.uniform(0, 500)  # ms
                auth_fails = np.random.randint(0, 10)
                
                self.metrics_queue.put({
                    'cpu': cpu,
                    'memory': mem,
                    'latency': latency,
                    'auth_failures': auth_fails
                })
                
                self.root.after(0, self._update_graphs)
                time.sleep(1)
                
            except Exception as e:
                print(f"Metrics update error: {e}")

    def _update_graphs(self):
        """Update metrics graphs"""
        try:
            metrics = self.metrics_queue.get_nowait()
            
            # Update CPU graph
            self._update_line(self.cpu_line, metrics['cpu'])
            
            # Update Memory graph
            self._update_line(self.mem_line, metrics['memory'])
            
            # Update Latency graph
            self._update_line(self.latency_line, metrics['latency'])
            
            # Update Auth Failures graph
            self._update_line(self.auth_line, metrics['auth_failures'])
            
            self.canvas.draw()
            
        except queue.Empty:
            pass

    def _update_line(self, line, new_value):
        """Update a single graph line"""
        xdata = list(line.get_xdata())
        ydata = list(line.get_ydata())
        
        if len(xdata) > 50:
            xdata = xdata[1:]
            ydata = ydata[1:]
            
        if not xdata:
            xdata = [0]
            ydata = [new_value]
        else:
            xdata.append(xdata[-1] + 1)
            ydata.append(new_value)
            
        line.set_data(xdata, ydata)
        line.axes.relim()
        line.axes.autoscale_view()

    def _add_user_dialog(self):
        """Show dialog for adding new user"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add User")
        dialog.geometry("300x200")
        
        ttk.Label(dialog, text="Username:").pack(pady=5)
        username_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=username_var).pack(pady=5)
        
        ttk.Label(dialog, text="Role:").pack(pady=5)
        role_var = tk.StringVar(value="reader")
        ttk.Combobox(
            dialog,
            textvariable=role_var,
            values=["admin", "writer", "reader"]
        ).pack(pady=5)
        
        def save_user():
            try:
                # Add user logic here
                self.users_tree.insert(
                    '',
                    'end',
                    values=(username_var.get(), role_var.get(), 'Active')
                )
                dialog.destroy()
            except Exception as e:
                messagebox.showerror("Error", str(e))
        
        ttk.Button(
            dialog,
            text="Save",
            style="Primary.TButton",
            command=save_user
        ).pack(pady=20)

    def _remove_user(self):
        """Remove selected user"""
        selected = self.users_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a user to remove")
            return
            
        if messagebox.askyesno("Confirm", "Remove selected user?"):
            self.users_tree.delete(selected)

    def run(self):
        """Start the application"""
        self._show_dashboard()  # Show dashboard by default
        self.root.mainloop()

if __name__ == "__main__":
    app = ModernIndexingUI()
    app.run()
