package cli

import (
	"fmt"
	"strings"

	"Lockr/bin/lsmtree"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/bubbles/table"
)

var (
	titleStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#8A2BE2")).
		Background(lipgloss.Color("#2F4F4F")).
		Padding(0, 1)

	statusMessageStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#9370DB"))

	errorMessageStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#FF0000"))

	tableStyle = lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("#8A2BE2"))

	headerStyle = lipgloss.NewStyle().
		Foreground(lipgloss.Color("#2F4F4F")).
		Background(lipgloss.Color("#8A2BE2")).
		Bold(true)
)

type item struct {
	key, value string
}

func (i item) Title() string       { return i.key }
func (i item) Description() string { return i.value }
func (i item) FilterValue() string { return i.key }

type model struct {
	lsm           *lsmtree.LSMTree
	input         textinput.Model
	table         table.Model
	statusMessage string
	errorMessage  string
	showTable     bool
	quitting      bool
}

func initialModel(lsm *lsmtree.LSMTree) model {
	ti := textinput.New()
	ti.Placeholder = "Enter command (e.g., set foo bar, get foo, delete foo, list, help)"
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 80
	ti.PlaceholderStyle = ti.PlaceholderStyle.Foreground(lipgloss.Color("#708090"))

	t := table.New(
		table.WithColumns([]table.Column{
			{Title: "Key", Width: 30},
			{Title: "Value", Width: 30},
		}),
		table.WithFocused(true),
		table.WithHeight(5),  // Reduced default height
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("#8A2BE2")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("#FFFFFF")).
		Background(lipgloss.Color("#8A2BE2")).
		Bold(true)
	t.SetStyles(s)

	return model{
		lsm:       lsm,
		input:     ti,
		table:     t,
		showTable: false,
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			m.quitting = true
			return m, tea.Quit
		case tea.KeyEnter:
			m.statusMessage = ""
			m.errorMessage = ""
			m.showTable = false
			m.executeCommand(m.input.Value())
			m.input.SetValue("")
			return m, nil
		case tea.KeyUp, tea.KeyDown:
			if m.showTable {
				if msg.Type == tea.KeyUp {
					m.table.MoveUp(1)
				} else {
					m.table.MoveDown(1)
				}
				return m, nil
			}
		}
	case tea.WindowSizeMsg:
		// Adjust the table height based on the window size, but keep it small
		newHeight := msg.Height / 4 // Use 1/4 of the window height
		if newHeight < 3 {
			newHeight = 3 // Minimum height of 3 rows
		} else if newHeight > 10 {
			newHeight = 10 // Maximum height of 10 rows
		}
		m.table.SetHeight(newHeight)
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m model) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("Lockr - Simple Key-Value Store"))
	b.WriteString("\n\n")

	b.WriteString(m.input.View())
	b.WriteString("\n\n")

	if m.statusMessage != "" {
		b.WriteString(statusMessageStyle.Render(m.statusMessage))
		b.WriteString("\n\n")
	}

	if m.errorMessage != "" {
		b.WriteString(errorMessageStyle.Render(m.errorMessage))
		b.WriteString("\n\n")
	}

	if m.showTable {
		b.WriteString(tableStyle.Render(m.table.View()))
	}

	return b.String()
}

func (m *model) executeCommand(input string) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		m.errorMessage = "Error: Empty command"
		return
	}

	command := parts[0]
	switch command {
	case "set":
		if len(parts) != 3 {
			m.errorMessage = "Error: Invalid set command. Usage: set <key> <value>"
			return
		}
		key, value := parts[1], parts[2]
		err := m.lsm.Set(key, value)
		if err != nil {
			m.errorMessage = fmt.Sprintf("Error: %v", err)
			return
		}
		m.statusMessage = fmt.Sprintf("Set %s to %s", key, value)

	case "get":
		if len(parts) != 2 {
			m.errorMessage = "Error: Invalid get command. Usage: get <key>"
			return
		}
		key := parts[1]
		value, err := m.lsm.Get(key)
		if err != nil {
			m.errorMessage = fmt.Sprintf("Error: %v", err)
			return
		}
		if value == "" {
			m.statusMessage = fmt.Sprintf("Key %s not found", key)
		} else {
			m.statusMessage = fmt.Sprintf("%s: %s", key, value)
		}

	case "delete":
		if len(parts) != 2 {
			m.errorMessage = "Error: Invalid delete command. Usage: delete <key>"
			return
		}
		key := parts[1]
		err := m.lsm.Delete(key)
		if err != nil {
			m.errorMessage = fmt.Sprintf("Error: %v", err)
			return
		}
		m.statusMessage = fmt.Sprintf("Deleted %s", key)

	case "list":
		entries, err := m.lsm.List()
		if err != nil {
			m.errorMessage = fmt.Sprintf("Error listing entries: %v", err)
			return
		}
		rows := []table.Row{}
		for k, v := range entries {
			rows = append(rows, table.Row{k, v})
		}
		m.table.SetRows(rows)
		m.showTable = true
		if len(rows) == 0 {
			m.statusMessage = "No items found"
		} else {
			m.statusMessage = fmt.Sprintf("Listed %d items. Use arrow keys to navigate.", len(rows))
		}

	case "help":
		m.showTable = false
		m.statusMessage = `Available commands:
- set <key> <value>: Set a key-value pair
- get <key>: Retrieve the value for a given key
- delete <key>: Delete a key-value pair
- list: Show all key-value pairs
- help: Display this help message`

	default:
		m.errorMessage = "Error: Invalid command. Use set, get, delete, list, or help"
	}
}

func RunUI(lsm *lsmtree.LSMTree) error {
	p := tea.NewProgram(initialModel(lsm), tea.WithAltScreen())
	_, err := p.Run()
	return err
}
