class ProgressTracker {
    static STORAGE_KEY = 'kfkvl_progress';

    static getProgress() {
        try {
            const data = localStorage.getItem(this.STORAGE_KEY);
            return data ? JSON.parse(data) : {};
        } catch (e) {
            return {};
        }
    }

    static markComplete(expId) {
        const progress = this.getProgress();
        progress[expId] = true;
        localStorage.setItem(this.STORAGE_KEY, JSON.stringify(progress));
    }

    static isComplete(expId) {
        return !!this.getProgress()[expId];
    }
}

class KnowledgeCheck {
    /**
     * Injects and displays a Knowledge Check modal.
     * @param {string} expId The experiment ID (e.g., 'exp-1', 'exp-2')
     * @param {string} question The question text
     * @param {string[]} options Array of 3-4 option strings
     * @param {number} correctIndex The index of the correct option (0-based)
     */
    static show(expId, question, options, correctIndex) {
        if (document.getElementById('kc-modal')) {
            document.getElementById('kc-modal').remove();
        }

        const modalHtml = `
            <div id="kc-modal" class="fixed inset-0 z-[100] flex items-center justify-center bg-background-dark/80 backdrop-blur-sm opacity-0 transition-opacity duration-300">
                <div class="bg-card-dark border border-slate-700 rounded-2xl p-8 max-w-md w-full shadow-2xl transform scale-95 transition-transform duration-300 relative">
                    
                    <div id="kc-content">
                        <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-primary/20 text-primary text-xs font-bold uppercase tracking-wider mb-4">
                            <span class="material-symbols-outlined !text-sm">quiz</span>
                            Knowledge Check
                        </div>
                        <h2 class="text-xl font-bold text-white mb-6 leading-snug">${question}</h2>
                        <div class="space-y-3" id="kc-options">
                            ${options.map((opt, i) => `
                                <button onclick="KnowledgeCheck.selectOption(${i}, ${correctIndex}, '${expId}')" class="w-full text-left p-4 rounded-xl border border-slate-700 bg-slate-800/50 hover:bg-slate-700/50 text-slate-300 hover:text-white transition-colors duration-200 flex items-start gap-3">
                                    <div class="w-6 h-6 rounded-full border border-slate-500 flex items-center justify-center flex-shrink-0 mt-0.5" id="opt-icon-${i}">
                                        <span class="text-xs">${String.fromCharCode(65 + i)}</span>
                                    </div>
                                    <span class="text-sm font-medium leading-relaxed">${opt}</span>
                                </button>
                            `).join('')}
                        </div>
                        <div id="kc-feedback" class="mt-4 text-sm font-bold text-center hidden"></div>
                    </div>

                    <!-- Success State -->
                    <div id="kc-success" class="hidden flex-col items-center text-center py-6">
                        <div class="w-20 h-20 rounded-full bg-emerald-500/20 text-emerald-400 flex items-center justify-center mb-6">
                            <span class="material-symbols-outlined !text-5xl">verified</span>
                        </div>
                        <h2 class="text-2xl font-bold text-white mb-2">Experiment Complete!</h2>
                        <p class="text-slate-400 text-sm mb-8">Your progress has been saved. Great job!</p>
                        <div class="flex flex-col gap-3 w-full">
                            <a href="experiments.html" class="w-full flex items-center justify-center gap-2 bg-emerald-600 hover:bg-emerald-500 text-white font-bold py-3 px-6 rounded-xl transition-all">
                                <span class="material-symbols-outlined">menu_book</span>
                                Return to Catalog
                            </a>
                            <button onclick="KnowledgeCheck.close()" class="w-full flex items-center justify-center gap-2 bg-slate-800 hover:bg-slate-700 text-slate-300 font-bold py-3 px-6 rounded-xl transition-all border border-slate-700">
                                <span class="material-symbols-outlined">play_arrow</span>
                                Stay in Experiment
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // Trigger animations
        setTimeout(() => {
            const modal = document.getElementById('kc-modal');
            modal.classList.remove('opacity-0');
            modal.querySelector('div').classList.remove('scale-95');
        }, 10);
    }

    static selectOption(selectedIndex, correctIndex, expId) {
        const buttons = document.querySelectorAll('#kc-options button');
        const feedback = document.getElementById('kc-feedback');
        
        buttons.forEach(btn => btn.disabled = true); // Disable further clicks

        if (selectedIndex === correctIndex) {
            // Correct
            buttons[selectedIndex].classList.add('bg-emerald-500/20', 'border-emerald-500', 'text-emerald-400');
            document.getElementById(`opt-icon-${selectedIndex}`).innerHTML = '<span class="material-symbols-outlined !text-sm">check</span>';
            document.getElementById(`opt-icon-${selectedIndex}`).classList.add('bg-emerald-500', 'border-emerald-500', 'text-white');
            
            feedback.textContent = "Correct!";
            feedback.className = "mt-4 text-sm font-bold text-center text-emerald-400 block";
            
            ProgressTracker.markComplete(expId);

            setTimeout(() => {
                document.getElementById('kc-content').classList.add('hidden');
                document.getElementById('kc-success').classList.remove('hidden');
                document.getElementById('kc-success').classList.add('flex');
            }, 1000);

        } else {
            // Incorrect
            buttons[selectedIndex].classList.add('bg-rose-500/20', 'border-rose-500', 'text-rose-400');
            document.getElementById(`opt-icon-${selectedIndex}`).innerHTML = '<span class="material-symbols-outlined !text-sm">close</span>';
            document.getElementById(`opt-icon-${selectedIndex}`).classList.add('bg-rose-500', 'border-rose-500', 'text-white');
            
            feedback.textContent = "Incorrect! Try again.";
            feedback.className = "mt-4 text-sm font-bold text-center text-rose-400 block";
            
            setTimeout(() => {
                buttons.forEach(btn => {
                    btn.disabled = false;
                    btn.className = "w-full text-left p-4 rounded-xl border border-slate-700 bg-slate-800/50 hover:bg-slate-700/50 text-slate-300 hover:text-white transition-colors duration-200 flex items-start gap-3";
                });
                document.getElementById(`opt-icon-${selectedIndex}`).innerHTML = `<span class="text-xs">${String.fromCharCode(65 + selectedIndex)}</span>`;
                document.getElementById(`opt-icon-${selectedIndex}`).className = "w-6 h-6 rounded-full border border-slate-500 flex items-center justify-center flex-shrink-0 mt-0.5";
                feedback.classList.add('hidden');
            }, 1500);
        }
    }

    static close() {
        const modal = document.getElementById('kc-modal');
        if (modal) {
            modal.classList.add('opacity-0');
            modal.querySelector('div').classList.add('scale-95');
            setTimeout(() => modal.remove(), 300);
        }
    }
}
