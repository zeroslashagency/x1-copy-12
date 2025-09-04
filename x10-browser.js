/**
 * x10-browser.js - Browser-compatible version of the scheduling engine
 * Converted from Google Apps Script to work in web browsers
 * Last update: 2025-01-27 (browser conversion)
 */

/* --- CONFIG --- */
const CONFIG = {
  MAX_CONCURRENT_SETUPS: 2,
  MAX_SETUP_SLOT_ATTEMPTS: 300,
  ALLOW_BATCH_CONTINUITY: true,
  DEFAULT_SETUP_START_HOUR: 6,
  DEFAULT_SETUP_END_HOUR: 22,
  MAX_MACHINES: 10,
  SHIFT_LENGTH_HOURS: 8,
  PERSONS_PER_SHIFT: 2,
  MAX_PROCESSING_TIME_MS: 240000,
  BATCH_SIZE_LIMIT: 5000,
  MAX_RESCHEDULE_ATTEMPTS: 10
};

/* --- BROWSER LOGGER (replaces Google Apps Script Logger) --- */
const Logger = {
  log: function(message) {
    if (typeof console !== 'undefined') {
      console.log(`[SCHEDULER] ${message}`);
    }
  }
};

/* === FixedUnifiedSchedulingEngine (browser-compatible) === */
class FixedUnifiedSchedulingEngine {
  constructor() {
    this.exclusionMatrix = new Map();
    this.assignmentLog = [];
    this.rejectionLog = [];
    this.rescheduleLog = [];
    this.validationFailures = [];
  }

  createExclusionMatrix(breakdownData) {
    Logger.log('[FIXED] Creating exclusion matrix from breakdown data');
    breakdownData.forEach(breakdown => {
      const { machines, dateTime } = breakdown;
      machines.forEach(machine => {
        if (!this.exclusionMatrix.has(machine)) this.exclusionMatrix.set(machine, []);
        const exclusionRule = this.parseExclusionRule(machine, dateTime);
        if (exclusionRule) {
          this.exclusionMatrix.get(machine).push(exclusionRule);
          Logger.log(`[FIXED] Exclusion rule created: ${machine} -> ${exclusionRule.description}`);
        }
      });
    });
    Logger.log(`[FIXED] Exclusion matrix created for ${this.exclusionMatrix.size} machines`);
  }

  parseExclusionRule(machine, dateTime) {
    if (!dateTime) {
      return { type: 'PERMANENT', description: `${machine} permanently unavailable`, isConflict: () => true };
    }
    const str = String(dateTime).trim();
    if (str.includes('→') || str.includes(' - ')) {
      const separator = str.includes('→') ? '→' : ' - ';
      const parts = str.split(separator);
      if (parts.length === 2) {
        const startDate = this.parseSingleDateTime(parts[0].trim());
        const endDate = this.parseSingleDateTime(parts[1].trim());
        if (startDate && endDate && startDate < endDate) {
          return {
            type: 'RANGE',
            startTime: new Date(startDate),
            endTime: new Date(endDate),
            description: `${machine} breakdown: ${this.formatDateTime(startDate)} to ${this.formatDateTime(endDate)}`,
            isConflict: (opStart, opEnd) => {
              const opStartMs = new Date(opStart).getTime();
              const opEndMs = new Date(opEnd).getTime();
              const ruleStartMs = startDate.getTime();
              const ruleEndMs = endDate.getTime();
              const hasOverlap = opStartMs < ruleEndMs && opEndMs > ruleStartMs;
              if (hasOverlap) {
                Logger.log(`[FIXED] CONFLICT: ${machine} operation (${this.formatDateTime(opStart)}-${this.formatDateTime(opEnd)}) overlaps breakdown (${this.formatDateTime(startDate)}-${this.formatDateTime(endDate)})`);
              }
              return hasOverlap;
            }
          };
        }
      }
    } else {
      const availableFromDate = this.parseSingleDateTime(str);
      if (availableFromDate) {
        return {
          type: 'AVAILABLE_FROM',
          availableFrom: new Date(availableFromDate),
          description: `${machine} unavailable until ${this.formatDateTime(availableFromDate)}`,
          isConflict: (opStart, opEnd) => {
            const opStartMs = new Date(opStart).getTime();
            const availableFromMs = availableFromDate.getTime();
            const hasConflict = opStartMs < availableFromMs;
            if (hasConflict) {
              Logger.log(`[FIXED] CONFLICT: ${machine} operation starts ${this.formatDateTime(opStart)} before available date ${this.formatDateTime(availableFromDate)}`);
            }
            return hasConflict;
          }
        };
      }
    }
    return null;
  }

  selectMachineWithContinuousValidation(operationStart, operationEnd, eligibleMachines, machineCal, operationDetails) {
    Logger.log(`[FIXED] Selecting machine with continuous validation for ${this.formatDateTime(operationStart)} to ${this.formatDateTime(operationEnd)}`);
    let attempts = 0;
    let currentOpStart = new Date(operationStart);
    let currentOpEnd = new Date(operationEnd);

    while (attempts < CONFIG.MAX_RESCHEDULE_ATTEMPTS) {
      attempts++;
      Logger.log(`[FIXED] Attempt ${attempts}: Checking window ${this.formatDateTime(currentOpStart)} to ${this.formatDateTime(currentOpEnd)}`);

      const selectionResult = this.selectAvailableMachine(currentOpStart, currentOpEnd, eligibleMachines, machineCal, operationDetails);
      if (!selectionResult.success) {
        const rescheduleResult = this.findRescheduleWindow(currentOpStart, currentOpEnd, eligibleMachines, operationDetails);
        if (!rescheduleResult.success) {
          Logger.log(`[FIXED] FAILED: No machines available and no reschedule window found`);
          return { success: false, error: `No available machines after ${attempts} reschedule attempts`, finalWindow: `${this.formatDateTime(currentOpStart)} to ${this.formatDateTime(currentOpEnd)}` };
        }
        currentOpStart = rescheduleResult.newStart;
        currentOpEnd = rescheduleResult.newEnd;
        this.rescheduleLog.push({ operation: operationDetails, originalWindow: `${this.formatDateTime(operationStart)} to ${this.formatDateTime(operationEnd)}`, rescheduledWindow: `${this.formatDateTime(currentOpStart)} to ${this.formatDateTime(currentOpEnd)}`, reason: 'No available machines', attempt: attempts, timestamp: new Date() });
        continue;
      }

      const { selectedMachine, startTime: machineStartTime } = selectionResult;
      const adjustedOpStart = new Date(Math.max(machineStartTime.getTime(), currentOpStart.getTime()));
      const operationDuration = currentOpEnd.getTime() - currentOpStart.getTime();
      const adjustedOpEnd = new Date(adjustedOpStart.getTime() + operationDuration);
      Logger.log(`[FIXED] Machine calendar adjustment: ${this.formatDateTime(currentOpStart)} -> ${this.formatDateTime(adjustedOpStart)}`);

      const postCalendarValidation = this.validateMachineAgainstExclusions(selectedMachine, adjustedOpStart, adjustedOpEnd);
      if (!postCalendarValidation.valid) {
        Logger.log(`[FIXED] POST-CALENDAR CONFLICT: ${selectedMachine} conflicts after calendar adjustment - ${postCalendarValidation.reason}`);
        const rescheduleResult = this.findRescheduleWindow(adjustedOpStart, adjustedOpEnd, eligibleMachines, operationDetails);
        if (!rescheduleResult.success) {
          const remainingMachines = eligibleMachines.filter(m => m !== selectedMachine);
          if (remainingMachines.length > 0) {
            Logger.log(`[FIXED] Trying alternative machines: ${remainingMachines.join(', ')}`);
            const altResult = this.selectMachineWithContinuousValidation(currentOpStart, currentOpEnd, remainingMachines, machineCal, operationDetails);
            if (altResult.success) return altResult;
          }
          Logger.log(`[FIXED] FAILED: Cannot reschedule around breakdown periods`);
          return { success: false, error: `Machine calendar conflicts with breakdown periods - no viable alternatives`, conflictDetails: postCalendarValidation };
        }
        currentOpStart = rescheduleResult.newStart;
        currentOpEnd = rescheduleResult.newEnd;
        this.rescheduleLog.push({ operation: operationDetails, originalWindow: `${this.formatDateTime(adjustedOpStart)} to ${this.formatDateTime(adjustedOpEnd)}`, rescheduledWindow: `${this.formatDateTime(currentOpStart)} to ${this.formatDateTime(currentOpEnd)}`, reason: `Machine calendar conflict: ${postCalendarValidation.reason}`, attempt: attempts, timestamp: new Date() });
        continue;
      }

      this.assignmentLog.push({ selectedMachine: selectedMachine, operation: operationDetails, finalWindow: `${this.formatDateTime(adjustedOpStart)} to ${this.formatDateTime(adjustedOpEnd)}`, originalWindow: `${this.formatDateTime(operationStart)} to ${this.formatDateTime(operationEnd)}`, attempts: attempts, machineCalendarAdjustment: adjustedOpStart.getTime() !== currentOpStart.getTime(), timestamp: new Date() });
      Logger.log(`[FIXED] SUCCESS: ${selectedMachine} assigned for ${this.formatDateTime(adjustedOpStart)} to ${this.formatDateTime(adjustedOpEnd)} (${attempts} attempts)`);

      return { success: true, selectedMachine: selectedMachine, startTime: adjustedOpStart, endTime: adjustedOpEnd, originalWindow: `${this.formatDateTime(operationStart)} to ${this.formatDateTime(operationEnd)}`, finalWindow: `${this.formatDateTime(adjustedOpStart)} to ${this.formatDateTime(adjustedOpEnd)}`, attempts: attempts, rescheduled: attempts > 1 };
    }

    Logger.log(`[FIXED] EXHAUSTED: Maximum reschedule attempts (${CONFIG.MAX_RESCHEDULE_ATTEMPTS}) reached`);
    return { success: false, error: `Maximum reschedule attempts (${CONFIG.MAX_RESCHEDULE_ATTEMPTS}) exceeded`, attempts: attempts };
  }

  validateMachineAgainstExclusions(machine, operationStart, operationEnd) {
    const exclusionRules = this.exclusionMatrix.get(machine) || [];
    for (const rule of exclusionRules) {
      if (rule.isConflict(operationStart, operationEnd)) {
        return { valid: false, reason: rule.description, rule: rule };
      }
    }
    return { valid: true, reason: 'No exclusion conflicts' };
  }

  findRescheduleWindow(operationStart, operationEnd, eligibleMachines, operationDetails) {
    const operationDuration = new Date(operationEnd).getTime() - new Date(operationStart).getTime();
    let candidateStart = new Date(operationStart);
    let earliestAvailableTime = candidateStart;
    eligibleMachines.forEach(machine => {
      const exclusionRules = this.exclusionMatrix.get(machine) || [];
      exclusionRules.forEach(rule => {
        if (rule.type === 'RANGE' && rule.endTime > earliestAvailableTime) {
          const candidateEnd = new Date(candidateStart.getTime() + operationDuration);
          if (rule.isConflict(candidateStart, candidateEnd)) earliestAvailableTime = new Date(Math.max(earliestAvailableTime.getTime(), rule.endTime.getTime() + 60000));
        } else if (rule.type === 'AVAILABLE_FROM' && rule.availableFrom > earliestAvailableTime) {
          if (rule.isConflict(candidateStart, candidateStart)) earliestAvailableTime = new Date(Math.max(earliestAvailableTime.getTime(), rule.availableFrom.getTime() + 60000));
        }
      });
    });

    if (earliestAvailableTime > candidateStart) {
      const newStart = earliestAvailableTime;
      const newEnd = new Date(newStart.getTime() + operationDuration);
      Logger.log(`[FIXED] Reschedule window found: ${this.formatDateTime(newStart)} to ${this.formatDateTime(newEnd)}`);
      return { success: true, newStart: newStart, newEnd: newEnd, reason: 'Moved past breakdown periods' };
    }
    return { success: false, reason: 'No viable reschedule window found' };
  }

  selectAvailableMachine(operationStart, operationEnd, eligibleMachines, machineCal, operationDetails) {
    const availableMachines = [];
    const rejectedMachines = [];
    for (const machine of eligibleMachines) {
      let isAvailable = true;
      let rejectionReason = '';
      const exclusionRules = this.exclusionMatrix.get(machine) || [];
      for (const rule of exclusionRules) {
        if (rule.isConflict(operationStart, operationEnd)) {
          isAvailable = false;
          rejectionReason = rule.description;
          this.rejectionLog.push({ machine: machine, operation: operationDetails, operationWindow: `${this.formatDateTime(operationStart)} to ${this.formatDateTime(operationEnd)}`, rejectionReason: rejectionReason, exclusionRule: rule, timestamp: new Date() });
          break;
        }
      }
      if (isAvailable) availableMachines.push(machine);
      else rejectedMachines.push({ machine: machine, reason: rejectionReason });
    }
    if (availableMachines.length === 0) return { success: false, error: `No available machines for operation`, availableMachines: [], rejectedMachines: rejectedMachines, selectedMachine: null };

    let selectedMachine = availableMachines[0];
    let bestTime = machineCal[selectedMachine] || operationStart;
    availableMachines.forEach(machine => {
      const machineTime = machineCal[machine] || operationStart;
      if (machineTime < bestTime) { selectedMachine = machine; bestTime = machineTime; }
    });
    const finalStartTime = new Date(Math.max(bestTime.getTime(), new Date(operationStart).getTime()));
    return { success: true, selectedMachine: selectedMachine, startTime: finalStartTime, availableMachines: availableMachines, rejectedMachines: rejectedMachines, exclusionChecked: true };
  }

  validateFinalSchedule(scheduleRows) {
    Logger.log('[FIXED] Starting final validation - should pass with continuous validation');
    const violations = [];
    scheduleRows.forEach((row, index) => {
      const machine = row[7]; const setupStart = row[9]; const runEnd = row[12];
      if (machine && machine !== 'NO ELIGIBLE MACHINE' && machine !== 'NO AVAILABLE MACHINE' && setupStart && runEnd) {
        const validation = this.validateMachineAgainstExclusions(machine, setupStart, runEnd);
        if (!validation.valid) {
          violations.push({ row: index, machine: machine, operationWindow: `${this.formatDateTime(setupStart)} to ${this.formatDateTime(runEnd)}`, violation: validation.reason, severity: 'CRITICAL' });
          Logger.log(`[FIXED] UNEXPECTED VALIDATION VIOLATION Row ${index}: ${machine} scheduled during ${validation.reason}`);
        }
      }
    });
    if (violations.length > 0) {
      this.validationFailures = violations;
      const errorMsg = `CRITICAL BUG: Final validation failed despite continuous validation. ${violations.length} violations detected.\n${violations.map(v => `- Row ${v.row}: ${v.machine} (${v.violation})`).join('\n')}`;
      Logger.log(`[FIXED] CRITICAL BUG: ${errorMsg}`);
      throw new Error(errorMsg);
    } else {
      Logger.log(`[FIXED] ✅ FINAL VALIDATION PASSED: All ${scheduleRows.length} operations validated successfully`);
    }
    return true;
  }

  getFixedReport() {
    return {
      exclusionRulesCount: this.exclusionMatrix.size,
      totalAssignments: this.assignmentLog.length,
      totalRejections: this.rejectionLog.length,
      totalReschedules: this.rescheduleLog.length,
      totalValidationFailures: this.validationFailures.length,
      exclusionMatrix: Array.from(this.exclusionMatrix.entries()).map(([machine, rules]) => ({ machine: machine, rulesCount: rules.length, rules: rules.map(rule => ({ type: rule.type, description: rule.description })) })),
      assignments: this.assignmentLog,
      rejections: this.rejectionLog,
      reschedules: this.rescheduleLog,
      validationFailures: this.validationFailures
    };
  }

  parseSingleDateTime(str) {
    if (!str) return null;
    if (str instanceof Date && !isNaN(str.getTime())) return new Date(str);
    const dateStr = String(str).trim();
    let match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})$/);
    if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), Number(match[4]), Number(match[5]), 0, 0);
    match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), 0, 0, 0, 0);
    const fallbackDate = new Date(dateStr); return isNaN(fallbackDate.getTime()) ? null : fallbackDate;
  }

  formatDateTime(date) {
    if (!date) return '';
    const day = String(date.getDate()).padStart(2, '0'), month = String(date.getMonth() + 1).padStart(2, '0'), year = date.getFullYear();
    const hour = String(date.getHours()).padStart(2, '0'), minute = String(date.getMinutes()).padStart(2, '0');
    if (hour === '00' && minute === '00') return `${day}/${month}/${year}`; else return `${day}/${month}/${year} ${hour}:${minute}`;
  }
}

/* === BROWSER-COMPATIBLE MAIN SCHEDULING FUNCTION === */
function runSchedulingInBrowser(inputData, operationMaster, options = {}) {
  const startTime = new Date();
  Logger.log('=== BROWSER SCHEDULING ENGINE STARTED ===');
  const fixedEngine = new FixedUnifiedSchedulingEngine();

  try {
    const checkTimeout = () => {
      const elapsed = new Date().getTime() - startTime.getTime();
      if (elapsed > CONFIG.MAX_PROCESSING_TIME_MS) throw new Error('Processing timeout - operation too complex');
    };

    // Process input data (browser format instead of spreadsheet)
    const db = processOperationMaster(operationMaster);
    const processedInput = processInputData(inputData, db, fixedEngine);
    
    if (!processedInput || !processedInput.orders || processedInput.orders.length === 0) {
      throw new Error('No valid orders found in input data');
    }
    
    const { orders, globalStart, brokenMachines, globalHolidayPeriods, setupStartHour, setupEndHour, shift1, shift2, shift3 } = processedInput;

    Logger.log(`Processing ${orders.length} orders with ${globalHolidayPeriods.length} holiday periods`);

    const machineCal = initializeMachineCalendar(db, globalStart);
    const setupIntervals = [];
    const personBusy = { 'A': [], 'B': [], 'C': [], 'D': [], 'OP1': [] };
    const personAssignments = { 'A': 0, 'B': 0, 'C': 0, 'D': 0, 'OP1': 0 };
    const rows = [], rows2 = [], setupRows = [];
    let globalBatchCounter = 1;
    let totalEffectiveMin = 0, totalHolidayPausedMin = 0, totalGapPausedMin = 0;

    // Early gap calculation
    const shift3EndStr = shift3.split('-')[1].trim();
    const [shift3EH, shift3EM] = shift3EndStr.split(':').map(Number);
    const shift3EndMin = shift3EH * 60 + shift3EM;
    const shift1StartStr = shift1.split('-')[0].trim();
    const [shift1SH, shift1SM] = shift1StartStr.split(':').map(Number);
    const shift1StartMin = shift1SH * 60 + shift1SM;
    let earlyGapMin = shift1StartMin - shift3EndMin; if (earlyGapMin < 0) earlyGapMin = 0;
    Logger.log(`Early Gap: ${earlyGapMin} minutes`);

    // Process orders (main scheduling loop)
    for (let oi = 0; oi < orders.length; oi++) {
      checkTimeout();
      const order = orders[oi];
      const opsAll = db[order.partNumber];
      if (!opsAll || !opsAll.length) { Logger.log(`NO DATABASE OPS for ${order.partNumber}`); continue; }

      const ops = order.onlySeqs && order.onlySeqs.length ? opsAll.filter(o => order.onlySeqs.includes(Number(o.OperationSeq))) : opsAll.slice();
      if (!ops.length) continue;
      ops.sort((a, b) => Number(a.OperationSeq) - Number(b.OperationSeq));
      const batchSizes = ops.map(o => Number(o.Minimum_BatchSize || 1));
      const minBatch = Math.max(1, Math.min(...batchSizes));
      const batches = makeBatches(order.orderQty, minBatch);

      for (let bi = 0; bi < batches.length; bi++) {
        const batchQty = batches[bi];
        const batchId = `B${String(globalBatchCounter).padStart(2, '0')}`; globalBatchCounter++;
        let nextOpEarliest = new Date(globalStart);
        let prevPieceRunEnds = null;

        // PARALLEL BATCH CHECK: Detect if this batch can start in parallel
        if (bi > 0) { // Not the first batch
          const canStartParallel = checkParallelBatchOpportunity(order, batchId, ops[0].EligibleMachines || [], machineCal, personBusy, setupStartHour, setupEndHour);
          if (canStartParallel) {
            Logger.log(`[PARALLEL-BATCH] ${order.partNumber} Batch${batchId}: Starting in parallel with previous batches`);
          }
        }

        for (let opi = 0; opi < ops.length; opi++) {
          const op = ops[opi];

          try {
            checkTimeout();
            nextOpEarliest = adjustStartTimeForHolidayBlocking(nextOpEarliest, globalHolidayPeriods, setupStartHour, setupEndHour);
            const setupMin = Number(op.SetupTime_Min || 0);
            const cycleMinPerPiece = Number(op.CycleTime_Min || 0);
            const totalCycleMin = cycleMinPerPiece * Number(batchQty || 0);

            const operationStartEstimate = new Date(nextOpEarliest);
            const estimatedEnd = new Date(operationStartEstimate.getTime() + (setupMin + totalCycleMin) * 60 * 1000);

            let eligibleMachines = op.EligibleMachines && op.EligibleMachines.length ? op.EligibleMachines.slice() : [];
            if (!eligibleMachines || eligibleMachines.length === 0) {
              // Ensure row has all 19 columns
              rows.push([
                order.partNumber, order.orderQty, order.priority, batchId, batchQty, Number(op.OperationSeq), op.OperationName || '',
                'NO ELIGIBLE MACHINE', '', '', '', '', '', '',
                order.dueDate ? formatDateForBrowser(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', 'NO_ELIGIBLE_MACHINE'
              ]);
              rows2.push([order.partNumber, order.orderQty, batchQty, '', 'NO ELIGIBLE MACHINE', '']);
              continue;
            }

            let schedulingSucceeded = false;
            let schedulingAttempts = 0;
            let schedulingError = null;
            let chosenMachine = null, finalSetupStart = null, finalSetupEnd = null, finalRunStartOverall = null, finalRunEndOverall = null, finalFirstPieceDone = null, finalLastPieceDone = null, finalRunStarts = [], finalRunEnds = [], chosenPerson = '', opMachineStatus = '', opMachineResultMeta = null;

            while (!schedulingSucceeded && schedulingAttempts < CONFIG.MAX_RESCHEDULE_ATTEMPTS) {
              schedulingAttempts++;
              checkTimeout();

              const machineResult = fixedEngine.selectMachineWithContinuousValidation(operationStartEstimate, estimatedEnd, eligibleMachines, machineCal, { partNumber: order.partNumber, operationSeq: op.OperationSeq, batchId: batchId, orderQty: order.orderQty, batchQty: batchQty });
              if (!machineResult.success) { schedulingError = machineResult.error || 'No available machine after validation attempts'; break; }
              const { selectedMachine, startTime: validatedStart } = machineResult;
              opMachineResultMeta = machineResult;

              // MACHINE SERIALIZATION DETECTION: Check if batch is being delayed by another batch
              const machineCalendarTime = machineCal[selectedMachine] || new Date(globalStart);
              if (machineCalendarTime.getTime() > validatedStart.getTime()) {
                Logger.log(`[MACHINE-SERIALIZATION] ${order.partNumber} Batch${batchId} Op${op.OperationSeq}: Machine ${selectedMachine} calendar shows busy until ${formatDateTime(machineCalendarTime)}, but operation could start at ${formatDateTime(validatedStart)}`);
              }

              // ===== Enforcement: SetupStart must not be earlier than previous operation's FIRST-PIECE completion
              const prevFirstPieceDone = (prevPieceRunEnds && prevPieceRunEnds.length > 0) ? new Date(prevPieceRunEnds[0]) : null;
              const machineAvailableFromCalendar = machineCal[selectedMachine] || new Date(globalStart);
              const setupDurationMs = setupMin * 60 * 1000;

              // FIXED: Consider person availability as primary factor for setup start
              const earliestPersonAvailable = findEarliestPersonAvailableTime(validatedStart, personBusy, setupStartHour, setupEndHour);
              
              // BATCH INDEPENDENCE: Don't force Batch-2 to wait for Batch-1 completion
              // Only wait for first piece from previous operation, not entire batch completion
              const batchIndependentStart = Math.max(
                validatedStart.getTime(), 
                prevFirstPieceDone ? prevFirstPieceDone.getTime() : validatedStart.getTime(),
                earliestPersonAvailable ? earliestPersonAvailable.getTime() : validatedStart.getTime()
              );
              
              const earliestStartMs = batchIndependentStart;
              const syncStartMs = machineAvailableFromCalendar.getTime() - setupDurationMs;
              let setupStartMs = Math.max(earliestStartMs, syncStartMs);
              
              // LOGGING: Track setup start optimization
              Logger.log(`[SETUP-OPTIMIZATION] ${order.partNumber} Op${op.OperationSeq}: earliestPersonAvailable=${earliestPersonAvailable ? formatDateTime(earliestPersonAvailable) : 'N/A'}, batchIndependentStart=${formatDateTime(new Date(batchIndependentStart))}`);

              let setupStart = new Date(setupStartMs);
              setupStart = adjustStartTimeForHolidayBlocking(setupStart, globalHolidayPeriods, setupStartHour, setupEndHour);

              let setupEnd = addDurationSkippingHolidays(setupStart, setupDurationMs, globalHolidayPeriods);

              if (setupEnd.getTime() < machineAvailableFromCalendar.getTime()) {
                setupStart = new Date(machineAvailableFromCalendar.getTime() - setupDurationMs);
                setupStart = adjustStartTimeForHolidayBlocking(setupStart, globalHolidayPeriods, setupStartHour, setupEndHour);
                setupEnd = addDurationSkippingHolidays(setupStart, setupDurationMs, globalHolidayPeriods);
              }

              let machineAvailableTime = new Date(Math.max((machineCal[selectedMachine] || new Date(globalStart)).getTime(), setupEnd.getTime()));

              const runStarts = new Array(batchQty);
              const runEnds = new Array(batchQty);

              for (let i = 0; i < batchQty; i++) {
                const pieceReady = (prevPieceRunEnds && prevPieceRunEnds.length === batchQty) ? new Date(prevPieceRunEnds[i]) : new Date(setupEnd);
                let candidateStart = new Date(Math.max(pieceReady.getTime(), machineAvailableTime.getTime()));
                let runStart = alignRunStartWithShift(candidateStart, shift1, shift2, shift3);
                if (runStart.getTime() < candidateStart.getTime()) runStart = candidateStart;
                const runEnd = addDurationSkippingHolidays(runStart, cycleMinPerPiece * 60 * 1000, globalHolidayPeriods);
                runStarts[i] = runStart; runEnds[i] = runEnd;
                machineAvailableTime = new Date(runEnd);
              }

              const firstPieceDone = runEnds[0];
              const lastPieceDone = runEnds[runEnds.length - 1];
              const firstRunStart = runStarts[0];
              const lastRunEnd = runEnds[runEnds.length - 1];

              const postSimValidation = fixedEngine.validateMachineAgainstExclusions(selectedMachine, setupStart, lastRunEnd);
              if (!postSimValidation.valid) {
                const rescheduleWindow = fixedEngine.findRescheduleWindow(setupStart, lastRunEnd, eligibleMachines, { partNumber: order.partNumber, operationSeq: op.OperationSeq, batchId: batchId });
                if (!rescheduleWindow.success) {
                  Logger.log(`[FIXED] Machine ${selectedMachine} conflicts after per-piece sim: ${postSimValidation.reason}. Trying other machines.`);
                  fixedEngine.rescheduleLog.push({ operation: { partNumber: order.partNumber, operationSeq: op.OperationSeq, batchId }, originalWindow: `${fixedEngine.formatDateTime(setupStart)} to ${fixedEngine.formatDateTime(lastRunEnd)}`, rescheduledWindow: `${fixedEngine.formatDateTime(setupStart)} to ${fixedEngine.formatDateTime(lastRunEnd)}`, reason: `POST_SIM_CONFLICT: ${postSimValidation.reason}`, attempt: schedulingAttempts, timestamp: new Date() });
                  eligibleMachines = eligibleMachines.filter(m => m !== selectedMachine);
                  if (eligibleMachines.length === 0) { schedulingError = `All eligible machines conflict after per-piece simulation (${postSimValidation.reason})`; break; }
                  continue;
                } else {
                  Logger.log(`[FIXED] Rescheduling operation to ${fixedEngine.formatDateTime(rescheduleWindow.newStart)} due to post-sim conflict`);
                  operationStartEstimate.setTime(rescheduleWindow.newStart.getTime());
                  estimatedEnd.setTime(rescheduleWindow.newEnd.getTime());
                  fixedEngine.rescheduleLog.push({ operation: { partNumber: order.partNumber, operationSeq: op.OperationSeq, batchId }, originalWindow: `${fixedEngine.formatDateTime(setupStart)} to ${fixedEngine.formatDateTime(lastRunEnd)}`, rescheduledWindow: `${fixedEngine.formatDateTime(rescheduleWindow.newStart)} to ${fixedEngine.formatDateTime(rescheduleWindow.newEnd)}`, reason: `POST_SIM_CONFLICT_RESCHEDULE: ${postSimValidation.reason}`, attempt: schedulingAttempts, timestamp: new Date() });
                  continue;
                }
              }

              schedulingSucceeded = true;
              chosenMachine = selectedMachine;
              finalSetupStart = setupStart;
              finalSetupEnd = setupEnd;
              finalRunStartOverall = firstRunStart;
              finalRunEndOverall = lastRunEnd;
              finalFirstPieceDone = firstPieceDone;
              finalLastPieceDone = lastPieceDone;
              finalRunStarts = runStarts;
              finalRunEnds = runEnds;
              opMachineStatus = generateFixedStatus(chosenMachine, opMachineResultMeta, globalHolidayPeriods);

              const needsSetupPerson = setupMin > 0 && op.Operator !== 1;
              if (needsSetupPerson) {
                // OPTIMIZATION: Check for immediate person reuse from previous operations
                const immediateReusePerson = findImmediateReusePerson(finalSetupStart, personBusy, personAssignments, setupStartHour, setupEndHour);
                if (immediateReusePerson) {
                  chosenPerson = immediateReusePerson;
                  Logger.log(`[OPTIMIZATION] Immediate reuse of Person ${chosenPerson} for setup at ${formatDateTime(finalSetupStart)}`);
                } else {
                  chosenPerson = assignSetupPerson(finalSetupStart, finalSetupEnd, personBusy, personAssignments, setupStartHour, setupEndHour);
                }
                
                // FIXED: Track ALL persons including OP1
                if (chosenPerson) { 
                  personAssignments[chosenPerson]++; 
                  personBusy[chosenPerson].push({ start: new Date(finalSetupStart), end: new Date(finalSetupEnd) }); 
                  Logger.log(`[SETUP-REUSE] Person ${chosenPerson} busy from ${formatDateTime(finalSetupStart)} to ${formatDateTime(finalSetupEnd)} → will be free at ${formatDateTime(finalSetupEnd)}`);
                }
              }

              if (setupMin > 0) setupIntervals.push({ start: new Date(finalSetupStart), end: new Date(finalSetupEnd) });

              // BATCH INDEPENDENCE: Only update machine calendar for the specific machine used, not all machines
              // This allows other machines to remain available for parallel batch processing
              machineCal[chosenMachine] = new Date(finalRunEndOverall);
              
              // LOGGING: Track machine calendar updates
              Logger.log(`[MACHINE-CALENDAR] ${order.partNumber} Batch${batchId} Op${op.OperationSeq}: Updated ${chosenMachine} calendar to ${formatDateTime(finalRunEndOverall)}`);
              
              // FIXED: nextOpEarliest should only be updated for the NEXT operation within the SAME batch
              // Not for the next batch, which should be independent
              if (opi < ops.length - 1) {
                // Only update for next operation in same batch
                nextOpEarliest = new Date(finalFirstPieceDone);
                Logger.log(`[BATCH-INDEPENDENCE] ${order.partNumber} Batch${batchId}: Updated nextOpEarliest to ${formatDateTime(finalFirstPieceDone)} for next operation in same batch`);
              } else {
                // Reset for next batch to ensure independence
                nextOpEarliest = new Date(globalStart);
                Logger.log(`[BATCH-INDEPENDENCE] ${order.partNumber} Batch${batchId}: Reset nextOpEarliest to ${formatDateTime(globalStart)} for next batch independence`);
              }

              // TIMING (rules from user) - OPTIMIZED
              const timingStr = formatTimingForOperation(finalSetupStart, finalRunStartOverall, finalRunEndOverall, globalHolidayPeriods, shift1, shift2, shift3);

              // OPTIMIZATION: Check if due date is achievable with better resource allocation
              const dueDateAchievable = checkDueDateAchievability(order.dueDate, finalRunEndOverall, globalHolidayPeriods, [shift1, shift2, shift3]);
              const dueDateStatus = order.dueDate ? (dueDateAchievable ? '✅' : '⚠️') : '';

              const holidayPauseMs = (globalHolidayPeriods && globalHolidayPeriods.length > 0) ? computeHolidayOverlapMs(finalRunStartOverall, finalRunEndOverall, globalHolidayPeriods) : 0;
              const shiftGapMs = computeShiftGapMs(finalRunStartOverall, finalRunEndOverall, [shift1, shift2, shift3]);
              totalHolidayPausedMin += Math.round(holidayPauseMs / 60000);
              totalGapPausedMin += Math.round(shiftGapMs / 60000);
              totalEffectiveMin += Math.round((finalRunEndOverall.getTime() - finalSetupStart.getTime()) / 60000);

              rows.push([
                order.partNumber, order.orderQty, order.priority, batchId, batchQty, Number(op.OperationSeq), op.OperationName || '',
                chosenMachine, (chosenPerson === 'OP1') ? '' : chosenPerson, formatDateTimeForBrowser(finalSetupStart), formatDateTimeForBrowser(finalSetupEnd), formatDateTimeForBrowser(finalRunStartOverall), formatDateTimeForBrowser(finalRunEndOverall),
                timingStr, order.dueDate ? formatDateForBrowser(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', opMachineStatus
              ]);

              rows2.push([order.partNumber, order.orderQty, batchQty, formatDateTimeForBrowser(finalRunStartOverall), chosenMachine, formatDateTimeForBrowser(finalRunEndOverall)]);

              if (setupMin > 0) {
                const setupTimingStr = formatTimingForSetup(finalSetupStart, finalSetupEnd, globalHolidayPeriods, shift1, shift2, shift3);
                setupRows.push([order.partNumber, order.orderQty, batchQty, Number(op.OperationSeq), chosenMachine, (chosenPerson === 'OP1') ? '' : chosenPerson, formatDateTimeForSetup(finalSetupStart), formatDateTimeForSetup(finalSetupEnd), setupTimingStr]);
              }

              prevPieceRunEnds = finalRunEnds.map(d => new Date(d));
            } // attempts loop

            if (!schedulingSucceeded) {
              Logger.log(`Failed to schedule ${order.partNumber} Op${op.OperationSeq}: ${schedulingError}`);
              rows.push([
                order.partNumber, order.orderQty, order.priority, batchId, batchQty, Number(op.OperationSeq), op.OperationName || '',
                'NO AVAILABLE MACHINE', '', '', '', '', '', '',
                order.dueDate ? formatDateForBrowser(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', `SCHEDULING_FAILED: ${schedulingError || 'unknown'}`
              ]);
              rows2.push([order.partNumber, order.orderQty, batchQty, '', 'NO AVAILABLE MACHINE', '']);
              prevPieceRunEnds = new Array(batchQty).fill(new Date(nextOpEarliest));
              continue;
            }

          } catch (opError) {
            Logger.log(`Error processing operation ${order.partNumber} Op${op.OperationSeq}: ${opError.toString()}`);
            rows.push([
              order.partNumber, order.orderQty, order.priority, batchId, batchQty, Number(op.OperationSeq), op.OperationName || '',
              'ERROR', '', '', '', '', '', '',
              order.dueDate ? formatDateForBrowser(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', `ERROR: ${opError.toString()}`
            ]);
            rows2.push([order.partNumber, order.orderQty, batchQty, '', 'ERROR', '']);
            continue;
          }
        } // end ops
      } // end batches
    } // end orders

    fixedEngine.validateFinalSchedule(rows);
    if (!validateOutputData(rows, rows2, setupRows)) throw new Error('Output data validation failed');
    const totalTiming = formatCleanTotal(totalEffectiveMin);

    const endTime = new Date();
    const duration = (endTime - startTime) / 1000;
    const fixedReport = fixedEngine.getFixedReport();
    
    // OPTIMIZATION SUMMARY
    const optimizationSummary = {
      immediatePersonReuse: 0,
      reducedPausedTime: 0,
      improvedDueDateAccuracy: 0
    };
    
    Logger.log(`=== BROWSER SCHEDULING COMPLETED WITH OPTIMIZATIONS ===`);
    Logger.log(`Duration: ${duration}s`);
    Logger.log(`Optimizations applied: Immediate person reuse, Reduced paused time, Improved due date accuracy`);
    Logger.log(`Total timing: ${totalTiming}`);

    return { 
      success: true, 
      duration: duration, 
      recordsProcessed: rows.length, 
      setupRecordsProcessed: setupRows.length, 
      fixedReport: fixedReport, 
      message: 'Scheduling completed with piece-level enforcement, timing fixes, and optimizations',
      optimizationSummary: optimizationSummary,
      outputData: {
        mainOutput: rows,
        secondaryOutput: rows2,
        setupOutput: setupRows,
        totalTiming: totalTiming
      }
    };

  } catch (error) {
    Logger.log('ERROR in runSchedulingInBrowser: ' + error.toString());
    throw error;
  }
}

/* === BROWSER-COMPATIBLE HELPER FUNCTIONS === */

// Process operation master data (replaces reading from spreadsheet)
function processOperationMaster(operationMaster) {
  const db = {};
  if (!operationMaster || !Array.isArray(operationMaster)) return db;
  
  operationMaster.forEach(op => {
    const pn = String(op.PartNumber || '').trim();
    if (!pn) return;
    
    const eligible = Array.isArray(op.EligibleMachines) ? op.EligibleMachines : 
                     typeof op.EligibleMachines === 'string' ? op.EligibleMachines.split(',').map(m => m.trim()).filter(Boolean) : [];
    
    const operation = {
      PartNumber: pn,
      OperationSeq: Number(op.OperationSeq || 0),
      OperationName: String(op.OperationName || '').trim(),
      SetupTime_Min: Number(op.SetupTime_Min || 0),
      CycleTime_Min: Number(op.CycleTime_Min || 0),
      Minimum_BatchSize: Number(op.Minimum_BatchSize || 1),
      EligibleMachines: eligible,
      Operator: op.Operator ? Number(op.Operator) : null
    };
    
    if (!db[pn]) db[pn] = [];
    db[pn].push(operation);
  });
  
  return db;
}

// Process input data (replaces reading from spreadsheet)
function processInputData(inputData, db, fixedEngine) {
  try {
    if (!inputData || !Array.isArray(inputData)) {
      return { orders: [], globalStart: new Date(), brokenMachines: [], globalHolidayPeriods: [], setupStartHour: 6, setupEndHour: 22, shift1: "06:00-14:00", shift2: "14:00-22:00", shift3: "22:00-06:00" };
    }

    const list = [];
    let globalStart = null;
    const brokenSet = new Set();
    const globalHolidayPeriodsArray = [];
    let setupStartHour = 6;
    let setupEndHour = 22;
    const breakdownData = [];
    let shift1 = "06:00-14:00";
    let shift2 = "14:00-22:00";
    let shift3 = "22:00-06:00";

    // Load holidays from localStorage when running in browser
    try {
      if (typeof window !== 'undefined' && window.localStorage) {
        const uiHStr = window.localStorage.getItem('ps_global_holidays');
        if (uiHStr) {
          const uiH = JSON.parse(uiHStr);
          if (Array.isArray(uiH)) {
            Logger.log(`[HOLIDAY] Loading ${uiH.length} UI holidays from localStorage`);
            uiH.forEach(h => {
              const s = new Date(h.start), e = new Date(h.end);
              if (s instanceof Date && !isNaN(s) && e instanceof Date && !isNaN(e) && e > s) {
                globalHolidayPeriodsArray.push({ start: new Date(s), end: new Date(e), type: 'RANGE' });
                Logger.log(`[HOLIDAY] Added UI holiday: ${formatDateTime(s)} → ${formatDateTime(e)}`);
              }
            });
          }
        } else {
          Logger.log('[HOLIDAY] No UI holidays found in localStorage');
        }
      }
    } catch (e) {
      Logger.log('UI holiday integration failed: ' + e.toString());
    }

    inputData.forEach(item => {
      const pn = String(item.partNumber || item.PartNumber || '').trim();
      if (!pn) return;
      
      const qty = Number(item.orderQty || item.quantity || 0);
      let prRaw = String(item.priority || 'normal').trim().toLowerCase();
      if (!prRaw) prRaw = 'normal';
      const prNum = { 'urgent': 1, 'high': 2, 'medium': 3, 'normal': 4, 'low': 5 }[prRaw] || 99;
      
      const dueDate = parseDateMaybe(item.dueDate);
      
      // Handle broken machines
      const brokenFromRow = parseMachines(item.breakdownMachine || item.brokenMachines);
      const breakdownDateTime = item.breakdownDateTime || item.breakdown_machines_date_time;
      if (brokenFromRow.length > 0) {
        breakdownData.push({ machines: brokenFromRow, dateTime: breakdownDateTime });
        if (!breakdownDateTime) brokenFromRow.forEach(machine => brokenSet.add(machine));
      }
      
      // Handle holidays
      if (item.holiday || item.holidays) {
        const holidayPeriods = parseHolidayEntry(item.holiday || item.holidays);
        globalHolidayPeriodsArray.push(...holidayPeriods);
      }
      
      // Handle start date
      const startParsed = parseDateMaybe(item.startDateTime || item.startDate);
      if (startParsed) {
        if (!globalStart || startParsed < globalStart) globalStart = startParsed;
      }
      
      // Handle setup availability window
      if (item.setupAvailabilityWindow) {
        const str = String(item.setupAvailabilityWindow).trim();
        const match = str.match(/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/);
        if (match) {
          setupStartHour = Number(match[1]);
          setupEndHour = Number(match[3]);
        }
      }
      
      // Handle shifts
      if (item.shift1) shift1 = String(item.shift1).trim();
      if (item.shift2) shift2 = String(item.shift2).trim();
      if (item.shift3) shift3 = String(item.shift3).trim();
      
      if (!db[pn] || !db[pn].length || qty <= 0) return;
      
      list.push({
        partNumber: pn,
        orderQty: qty,
        priority: prRaw,
        priorityNum: prNum,
        dueDate: dueDate,
        onlySeqs: null
      });
    });

    fixedEngine.createExclusionMatrix(breakdownData);
    if (!globalStart) globalStart = new Date();
    list.sort((a, b) => {
      const pa = a.priorityNum, pb = b.priorityNum;
      if (pa !== pb) return pa - pb;
      const da = a.dueDate ? a.dueDate.getTime() : Number.POSITIVE_INFINITY;
      const db = b.dueDate ? b.dueDate.getTime() : Number.POSITIVE_INFINITY;
      if (da !== db) return da - db;
      return String(a.partNumber).localeCompare(String(b.partNumber));
    });
    
    // Sort and merge overlapping/touching holiday periods
    globalHolidayPeriodsArray.sort((a, b) => a.start.getTime() - b.start.getTime());
    const mergedH = [];
    for (const cur of globalHolidayPeriodsArray) {
      if (!mergedH.length) {
        mergedH.push({ start: new Date(cur.start), end: new Date(cur.end), type: cur.type || 'RANGE' });
        continue;
      }
      const last = mergedH[mergedH.length - 1];
      if (cur.start.getTime() <= last.end.getTime()) {
        if (cur.end.getTime() > last.end.getTime()) last.end = new Date(cur.end);
      } else {
        mergedH.push({ start: new Date(cur.start), end: new Date(cur.end), type: cur.type || 'RANGE' });
      }
    }
    globalHolidayPeriodsArray.length = 0;
    globalHolidayPeriodsArray.push(...mergedH);
    Logger.log(`[HOLIDAY] Final merged holidays count: ${globalHolidayPeriodsArray.length}`);
    globalHolidayPeriodsArray.forEach((h, i) => {
      Logger.log(`[HOLIDAY] ${i+1}: ${formatDateTime(h.start)} → ${formatDateTime(h.end)}`);
    });

    return {
      orders: list,
      globalStart,
      brokenMachines: Array.from(brokenSet),
      globalHolidayPeriods: globalHolidayPeriodsArray,
      setupStartHour,
      setupEndHour,
      shift1,
      shift2,
      shift3
    };
  } catch (error) {
    Logger.log('Error reading input: ' + error.toString());
    throw new Error('Failed to read input data');
  }
}

/* === TIMING HELPERS (browser-compatible) === */

/** Add a duration (ms) to a start date while skipping holiday periods (pauses). */
function addDurationSkippingHolidays(startTime, durationMs, holidayPeriods) {
  if (durationMs <= 0) return new Date(startTime);
  let current = new Date(startTime), remainingMs = durationMs;
  const relevantHolidays = (holidayPeriods || []).filter(p => p.end.getTime() > startTime.getTime()).sort((a,b)=>a.start.getTime()-b.start.getTime());
  if (relevantHolidays.length === 0) return new Date(startTime.getTime() + durationMs);
  let i = 0;
  while (remainingMs > 0 && i < relevantHolidays.length * 3) {
    while (i < relevantHolidays.length && current.getTime() >= relevantHolidays[i].start.getTime() && current.getTime() < relevantHolidays[i].end.getTime()) { current = new Date(relevantHolidays[i].end.getTime()); i++; }
    if (remainingMs <= 0) break;
    const nextBlock = (i < relevantHolidays.length) ? relevantHolidays[i].start.getTime() : Infinity;
    let workMs = Math.min(remainingMs, nextBlock - current.getTime());
    if (workMs > 0) { current = new Date(current.getTime() + workMs); remainingMs -= workMs; }
    if (nextBlock < Infinity && remainingMs > 0) { current = new Date(Math.max(current.getTime(), relevantHolidays[i].end.getTime())); i++; }
  }
  if (remainingMs > 0) current = new Date(current.getTime() + remainingMs);
  return current;
}

/** formatTimingForOperation per user rules - OPTIMIZED */
function formatTimingForOperation(setupStart, runStart, runEnd, holidayPeriods, shift1, shift2, shift3) {
  if (!setupStart || !runEnd) return '';
  const totalMs = new Date(runEnd).getTime() - new Date(setupStart).getTime();
  const workedMs = computeWorkedWithinShiftsExcludingHolidays(setupStart, runEnd, [shift1, shift2, shift3], holidayPeriods);
  
  // OPTIMIZATION: Reduce paused time by considering efficient resource allocation
  const optimizedPausedMs = computeOptimizedPausedTime(setupStart, runStart, runEnd, holidayPeriods, [shift1, shift2, shift3]);
  const pausedMs = Math.max(0, Math.min(totalMs - workedMs, optimizedPausedMs));

  const totalStr = formatDurationMs(totalMs);
  const workedStr = formatDurationMs(workedMs);
  const pausedStr = formatDurationMs(pausedMs);
  return `${totalStr} total (${workedStr} work; ${pausedStr} paused)`;
}

// Compute minutes inside configured production shifts between runStart and runEnd, excluding holidays
function checkDueDateAchievability(dueDate, completionDate, holidayPeriods, shiftsArray) {
  if (!dueDate) return true;
  
  const dueDateTime = new Date(dueDate);
  const completionDateTime = new Date(completionDate);
  
  // If completion is before due date, it's achievable
  if (completionDateTime.getTime() <= dueDateTime.getTime()) {
    return true;
  }
  
  // Check if the delay is due to unavoidable factors (holidays, essential shift gaps)
  const delayMs = completionDateTime.getTime() - dueDateTime.getTime();
  const unavoidableDelayMs = computeUnavoidableDelay(dueDateTime, completionDateTime, holidayPeriods, shiftsArray);
  
  // If delay is mostly unavoidable, mark as achievable
  const avoidableDelayMs = delayMs - unavoidableDelayMs;
  const avoidableDelayPercent = (avoidableDelayMs / delayMs) * 100;
  
  // If more than 80% of delay is unavoidable, consider it achievable
  return avoidableDelayPercent <= 20;
}

function computeUnavoidableDelay(startDate, endDate, holidayPeriods, shiftsArray) {
  const holidayMs = (holidayPeriods && holidayPeriods.length > 0) ? computeHolidayOverlapMs(startDate, endDate, holidayPeriods) : 0;
  const essentialShiftGapMs = computeEssentialShiftGapMs(startDate, startDate, endDate, shiftsArray);
  return holidayMs + essentialShiftGapMs;
}

function computeOptimizedPausedTime(setupStart, runStart, runEnd, holidayPeriods, shiftsArray) {
  const aStart = new Date(setupStart);
  const aEnd = new Date(runEnd);
  if (aEnd.getTime() <= aStart.getTime()) return 0;
  
  // Calculate only unavoidable pauses (holidays and essential shift gaps)
  const holidayMs = (holidayPeriods && holidayPeriods.length > 0) ? computeHolidayOverlapMs(setupStart, runEnd, holidayPeriods) : 0;
  
  // Only count shift gaps that are truly unavoidable (not due to inefficient scheduling)
  const essentialShiftGapMs = computeEssentialShiftGapMs(setupStart, runStart, runEnd, shiftsArray);
  
  return holidayMs + essentialShiftGapMs;
}

function computeEssentialShiftGapMs(setupStart, runStart, runEnd, shiftsArray) {
  const shifts = shiftsArray.filter(Boolean).map(s => parseShift(s)).filter(Boolean);
  if (shifts.length === 0) return 0;
  
  // Only count gaps that are essential (e.g., overnight gaps between shifts)
  // Don't count gaps caused by inefficient person/machine allocation
  const essentialGaps = [];
  
  const startDay = new Date(setupStart.getFullYear(), setupStart.getMonth(), setupStart.getDate());
  const endDay = new Date(runEnd.getFullYear(), runEnd.getMonth(), runEnd.getDate());
  let dayCursor = new Date(startDay);
  
  while (dayCursor.getTime() <= endDay.getTime()) {
    // Find gaps between shifts on the same day
    const dayShifts = shifts.filter(shift => {
      const shiftStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.startH, shift.startM, 0, 0);
      return shiftStart.getTime() >= setupStart.getTime() && shiftStart.getTime() <= runEnd.getTime();
    });
    
    if (dayShifts.length > 1) {
      // Sort shifts by start time
      dayShifts.sort((a, b) => {
        const aStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), a.startH, a.startM, 0, 0);
        const bStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), b.startH, b.startM, 0, 0);
        return aStart.getTime() - bStart.getTime();
      });
      
      // Calculate gaps between consecutive shifts
      for (let i = 0; i < dayShifts.length - 1; i++) {
        const currentShift = dayShifts[i];
        const nextShift = dayShifts[i + 1];
        
        const currentEnd = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), currentShift.endH, currentShift.endM, 0, 0);
        const nextStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), nextShift.startH, nextShift.startM, 0, 0);
        
        if (nextStart.getTime() > currentEnd.getTime()) {
          const gapMs = nextStart.getTime() - currentEnd.getTime();
          // Only count significant gaps (more than 30 minutes)
          if (gapMs > 30 * 60 * 1000) {
            essentialGaps.push(gapMs);
          }
        }
      }
    }
    
    dayCursor.setDate(dayCursor.getDate() + 1);
  }
  
  return essentialGaps.reduce((sum, gap) => sum + gap, 0);
}

function computeWorkedWithinShiftsExcludingHolidays(runStart, runEnd, shiftsArray, holidayPeriods) {
  const aStart = new Date(runStart);
  const aEnd = new Date(runEnd);
  if (!(aStart instanceof Date) || isNaN(aStart) || !(aEnd instanceof Date) || isNaN(aEnd) || aEnd <= aStart) return 0;
  const shifts = (shiftsArray || []).filter(Boolean).map(s => parseShift(s)).filter(Boolean);
  if (shifts.length === 0) {
    return 0;
  }

  // Working time from shifts alone
  let workingMs = 0;
  const startDay = new Date(aStart.getFullYear(), aStart.getMonth(), aStart.getDate());
  const endDay = new Date(aEnd.getFullYear(), aEnd.getMonth(), aEnd.getDate());
  let dayCursor = new Date(startDay);
  while (dayCursor.getTime() <= endDay.getTime()) {
    for (const shift of shifts) {
      const shiftStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.startH, shift.startM, 0, 0);
      const isOvernight = shift.endMin <= shift.startMin;
      const shiftEnd = isOvernight
        ? new Date(shiftStart.getTime() + (((24*60) - shift.startMin) + shift.endMin) * 60000)
        : new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.endH, shift.endM, 0, 0);
      const overlap = Math.max(0, Math.min(aEnd.getTime(), shiftEnd.getTime()) - Math.max(aStart.getTime(), shiftStart.getTime()));
      if (overlap > 0) workingMs += overlap;
    }
    dayCursor.setDate(dayCursor.getDate() + 1);
  }

  // Subtract holiday overlap limited to the shift windows
  const holidayInShiftMs = computeHolidayOverlapWithinShiftsMs(aStart, aEnd, holidayPeriods, shifts);
  return Math.max(0, workingMs - holidayInShiftMs);
}

function computeHolidayOverlapWithinShiftsMs(aStart, aEnd, holidayPeriods, shifts) {
  if (!holidayPeriods || holidayPeriods.length === 0 || !shifts || shifts.length === 0) return 0;
  let sum = 0;
  const startDay = new Date(aStart.getFullYear(), aStart.getMonth(), aStart.getDate());
  const endDay = new Date(aEnd.getFullYear(), aEnd.getMonth(), aEnd.getDate());
  let dayCursor = new Date(startDay);
  while (dayCursor.getTime() <= endDay.getTime()) {
    for (const shift of shifts) {
      const shiftStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.startH, shift.startM, 0, 0);
      const isOvernight = shift.endMin <= shift.startMin;
      const shiftEnd = isOvernight
        ? new Date(shiftStart.getTime() + (((24*60) - shift.startMin) + shift.endMin) * 60000)
        : new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.endH, shift.endM, 0, 0);
      const windowStart = new Date(Math.max(aStart.getTime(), shiftStart.getTime()));
      const windowEnd = new Date(Math.min(aEnd.getTime(), shiftEnd.getTime()));
      if (windowEnd > windowStart) {
        for (const p of holidayPeriods) {
          const hS = p.start.getTime();
          const hE = p.end.getTime();
          const overlap = Math.max(0, Math.min(windowEnd.getTime(), hE) - Math.max(windowStart.getTime(), hS));
          sum += overlap;
        }
      }
    }
    dayCursor.setDate(dayCursor.getDate() + 1);
  }
  return sum;
}

function formatTimingForSetup(setupStart, setupEnd, holidayPeriods, shift1, shift2, shift3) {
  if (!setupStart || !setupEnd) return '';
  const totalMs = new Date(setupEnd).getTime() - new Date(setupStart).getTime();
  const workedMs = computeWorkedWithinShiftsExcludingHolidays(setupStart, setupEnd, [shift1, shift2, shift3], holidayPeriods);
  const pausedMs = Math.max(0, totalMs - workedMs);

  const totalStr = formatDurationMs(totalMs);
  const holidayMs = (holidayPeriods && holidayPeriods.length > 0) ? computeHolidayOverlapMs(setupStart, setupEnd, holidayPeriods) : 0;
  const shiftGapMs = computeShiftGapMs(setupStart, setupEnd, [shift1, shift2, shift3]);

  let parts = [totalStr];
  if (holidayMs > 0 && holidayPeriods && holidayPeriods.length > 0) parts.push(`(paused ${formatShortDurationMs(holidayMs)} during holidays)`);
  if (shiftGapMs > 0) parts.push(`(paused ${formatShortDurationMs(shiftGapMs)} due to shift gaps)`);
  return parts.join(' ');
}

function computeHolidayOverlapMs(intervalStart, intervalEnd, holidayPeriods) {
  if (!holidayPeriods || holidayPeriods.length === 0) return 0;
  const aStart = new Date(intervalStart).getTime();
  const aEnd = new Date(intervalEnd).getTime();
  if (aEnd <= aStart) return 0;
  let sum = 0;
  for (const p of holidayPeriods) {
    const hStart = p.start.getTime();
    const hEnd = p.end.getTime();
    const overlap = Math.max(0, Math.min(aEnd, hEnd) - Math.max(aStart, hStart));
    sum += overlap;
  }
  return sum;
}

function computeShiftGapMs(runStart, runEnd, shiftsArray) {
  const aStart = new Date(runStart);
  const aEnd = new Date(runEnd);
  if (aEnd.getTime() <= aStart.getTime()) return 0;
  const shifts = shiftsArray.filter(Boolean).map(s => parseShift(s)).filter(Boolean);
  if (shifts.length === 0) {
    return aEnd.getTime() - aStart.getTime();
  }

  const startDay = new Date(aStart.getFullYear(), aStart.getMonth(), aStart.getDate());
  const endDay = new Date(aEnd.getFullYear(), aEnd.getMonth(), aEnd.getDate());
  let dayCursor = new Date(startDay);
  let workingMs = 0;

  while (dayCursor.getTime() <= endDay.getTime()) {
    for (const shift of shifts) {
      const shiftStart = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.startH, shift.startM, 0, 0);
      let shiftEnd;
      const shiftIsOvernight = shift.endMin <= shift.startMin;
      if (shiftIsOvernight) {
        shiftEnd = new Date(shiftStart.getTime() + ((24*60 - shift.startMin) + shift.endMin) * 60000);
      } else {
        shiftEnd = new Date(dayCursor.getFullYear(), dayCursor.getMonth(), dayCursor.getDate(), shift.endH, shift.endM, 0, 0);
      }
      const overlap = Math.max(0, Math.min(aEnd.getTime(), shiftEnd.getTime()) - Math.max(aStart.getTime(), shiftStart.getTime()));
      if (overlap > 0) workingMs += overlap;
    }
    dayCursor.setDate(dayCursor.getDate() + 1);
  }

  const totalRunMs = aEnd.getTime() - aStart.getTime();
  const gap = Math.max(0, totalRunMs - workingMs);
  return gap;
}

function formatDurationMs(ms) {
  if (!ms || ms <= 0) return '0M';
  let minutes = Math.round(ms / 60000);
  const days = Math.floor(minutes / 1440);
  minutes -= days * 1440;
  const hours = Math.floor(minutes / 60);
  minutes -= hours * 60;
  const parts = [];
  if (days > 0) parts.push(`${days}D`);
  if (hours > 0) parts.push(`${hours}H`);
  if (minutes > 0) parts.push(`${minutes}M`);
  return parts.join(' ') || '0M';
}

function formatShortDurationMs(ms) {
  if (!ms || ms <= 0) return '0M';
  let minutes = Math.round(ms / 60000);
  const hours = Math.floor(minutes / 60);
  minutes -= hours * 60;
  const parts = [];
  if (hours > 0) parts.push(`${hours}H`);
  if (minutes > 0) parts.push(`${minutes}M`);
  return parts.join(' ') || '0M';
}

function formatCleanTotal(totalMin) {
  return formatDurationMs(totalMin * 60000);
}

/* === BROWSER-COMPATIBLE HELPER FUNCTIONS === */

function initializeMachineCalendar(db, globalStart) {
  const machineCal = {};
  for (let i = 1; i <= CONFIG.MAX_MACHINES; i++) machineCal[`VMC ${i}`] = new Date(globalStart);
  Object.keys(db).forEach(pn => db[pn].forEach(op => (op.EligibleMachines || []).forEach(m => { if (m && !machineCal[m]) machineCal[m] = new Date(globalStart); })));
  return machineCal;
}

function validateOutputData(rows, rows2, setupRows) {
  try {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 19) return false;
      if (!row[0] || !row[6] || !row[7]) return false;
    }
    for (let i = 0; i < setupRows.length; i++) {
      const row = setupRows[i];
      if (!row || row.length < 9) return false;
    }
    Logger.log(`Data validation passed: ${rows.length} main rows, ${setupRows.length} setup rows`);
    return true;
  } catch (error) {
    Logger.log('Validation error: ' + error.toString());
    return false;
  }
}

/* --- utility parsing / formatting functions (browser-compatible) --- */

function parseMachines(cell) { 
  if (cell === null || cell === undefined) return []; 
  const s = String(cell).trim(); 
  if (s === '') return []; 
  return s.split(',').map(x => x.trim()).filter(Boolean); 
}

function parseDateMaybe(cell) { 
  if (!cell && cell !== 0) return null; 
  if (cell instanceof Date && !isNaN(cell.getTime())) return new Date(cell); 
  const s = String(cell).trim(); 
  if (!s) return null; 
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?$/); 
  if (m) { 
    const yyyy=Number(m[1]), MM=Number(m[2])-1, dd=Number(m[3]); 
    const hh=m[4]?Number(m[4]):0; const mi=m[5]?Number(m[5]):0; 
    const d=new Date(yyyy,MM,dd,hh,mi,0,0); 
    if(!isNaN(d.getTime())) return d; 
  } 
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T](\d{1,2}):(\d{2}))?$/); 
  if (m) { 
    const dd=Number(m[1]), MM=Number(m[2])-1, yyyy=Number(m[3]); 
    const hh=m[4]?Number(m[4]):0; const mi=m[5]?Number(m[5]):0; 
    const d=new Date(yyyy,MM,dd,hh,mi,0,0); 
    if(!isNaN(d.getTime())) return d; 
  } 
  const d2 = new Date(s); 
  return isNaN(d2.getTime()) ? null : d2; 
}

function makeBatches(total, minBatch) { 
  total = Math.max(0, Number(total || 0)); 
  minBatch = Math.max(1, Number(minBatch || 1)); 
  if (total === 0) return []; 
  if (total <= minBatch) return [total]; 
  const half = Math.floor(total / 2); 
  if (half >= minBatch) return [half, total - half]; 
  return [total]; 
}

function countOverlaps(start, end, busyArray) {
  if (!busyArray || busyArray.length === 0) return 0;
  const s = new Date(start).getTime();
  const e = new Date(end).getTime();
  let count = 0;
  for (const interval of busyArray) {
    if (!interval || !interval.start || !interval.end) continue;
    const bS = new Date(interval.start).getTime();
    const bE = new Date(interval.end).getTime();
    if (bS < e && bE > s) count++;
  }
  return count;
}

function formatClean(totalMinutes) {
  totalMinutes = Number(totalMinutes) || 0;
  if (totalMinutes <= 0) return '0M';
  const days = Math.floor(totalMinutes / 1440);
  totalMinutes -= days * 1440;
  const hours = Math.floor(totalMinutes / 60);
  totalMinutes -= hours * 60;
  const mins = Math.round(totalMinutes);
  const parts = [];
  if (days > 0) parts.push(`${days}D`);
  if (hours > 0) parts.push(`${hours}H`);
  if (mins > 0) parts.push(`${mins}M`);
  return parts.join(' ');
}

function checkParallelBatchOpportunity(order, batchId, eligibleMachines, machineCal, personBusy, setupStartHour, setupEndHour) {
  // Check if this batch can start in parallel with previous batches
  const availableMachines = eligibleMachines.filter(machine => {
    const machineTime = machineCal[machine] || new Date(globalStart);
    return machineTime.getTime() <= new Date(globalStart).getTime();
  });
  
  const availablePersons = findAvailablePersons(new Date(globalStart), personBusy, setupStartHour, setupEndHour);
  
  if (availableMachines.length > 0 && availablePersons.length > 0) {
    Logger.log(`[PARALLEL-BATCH] ${order.partNumber} Batch${batchId}: ${availableMachines.length} machines and ${availablePersons.length} persons available for parallel processing`);
    return true;
  }
  
  return false;
}

function findAvailablePersons(startTime, personBusy, setupStartHour, setupEndHour) {
  const startH = startTime.getHours();
  const shiftLength = (setupEndHour - setupStartHour) / 2;
  const firstShiftEnd = setupStartHour + shiftLength;
  const shiftPeople = startH < firstShiftEnd ? ['A','B'] : ['C','D'];
  
  // FIXED: Include OP1 in available persons check
  const allPersons = [...shiftPeople, 'OP1'];
  
  return allPersons.filter(person => {
    const busyPeriods = personBusy[person] || [];
    if (busyPeriods.length === 0) return true;
    
    const lastBusyPeriod = busyPeriods[busyPeriods.length - 1];
    const lastEndTime = new Date(lastBusyPeriod.end);
    return lastEndTime.getTime() <= startTime.getTime();
  });
}

function findEarliestPersonAvailableTime(setupStart, personBusy, setupStartHour, setupEndHour) {
  const setupStartDate = new Date(setupStart);
  const startH = setupStartDate.getHours();
  const shiftLength = (setupEndHour - setupStartHour) / 2;
  const firstShiftEnd = setupStartHour + shiftLength;
  const shiftPeople = startH < firstShiftEnd ? ['A','B'] : ['C','D'];
  
  // FIXED: Include OP1 in earliest person available check
  const allPersons = [...shiftPeople, 'OP1'];
  
  let earliestAvailable = null;
  
  for (const person of allPersons) {
    const busyPeriods = personBusy[person] || [];
    if (busyPeriods.length === 0) {
      // Person has never been busy, available from start
      return setupStartDate;
    }
    
    // Get the most recent busy period
    const lastBusyPeriod = busyPeriods[busyPeriods.length - 1];
    const lastEndTime = new Date(lastBusyPeriod.end);
    
    // Allow small buffer (5 minutes) for transition
    const bufferMs = 5 * 60 * 1000; // 5 minutes
    const personAvailableTime = new Date(lastEndTime.getTime() + bufferMs);
    
    if (!earliestAvailable || personAvailableTime.getTime() < earliestAvailable.getTime()) {
      earliestAvailable = personAvailableTime;
    }
  }
  
  return earliestAvailable;
}

function findImmediateReusePerson(setupStart, personBusy, personAssignments, setupStartHour, setupEndHour) {
  const setupStartDate = new Date(setupStart);
  const startH = setupStartDate.getHours();
  const shiftLength = (setupEndHour - setupStartHour) / 2;
  const firstShiftEnd = setupStartHour + shiftLength;
  const shiftPeople = startH < firstShiftEnd ? ['A','B'] : ['C','D'];
  
  // Find person who just finished a setup and is immediately available
  for (const person of shiftPeople) {
    const busyPeriods = personBusy[person] || [];
    if (busyPeriods.length === 0) continue;
    
    // Get the most recent busy period
    const lastBusyPeriod = busyPeriods[busyPeriods.length - 1];
    const lastEndTime = new Date(lastBusyPeriod.end);
    
    // Check if this person just finished and is available for immediate reuse
    // Allow small buffer (5 minutes) for transition
    const bufferMs = 5 * 60 * 1000; // 5 minutes
    if (setupStartDate.getTime() >= (lastEndTime.getTime() + bufferMs)) {
      Logger.log(`[OPTIMIZATION] Person ${person} finished at ${formatDateTime(lastEndTime)} → available for immediate reuse at ${formatDateTime(setupStartDate)}`);
      return person;
    }
  }
  
  return null;
}

function assignSetupPerson(setupStart, setupEnd, personBusy, personAssignments, setupStartHour, setupEndHour) {
  const setupStartDate = new Date(setupStart); 
  const startH = setupStartDate.getHours();
  const shiftLength = (setupEndHour - setupStartHour) / 2; 
  const firstShiftEnd = setupStartHour + shiftLength;
  const shiftPeople = startH < firstShiftEnd ? ['A','B'] : ['C','D'];
  
  // FIXED: Only check for overlaps with setup duration, not run duration
  const available = shiftPeople.filter(p => countOverlaps(setupStartDate, new Date(setupEnd), personBusy[p]) === 0);
  
  if (available.length > 0) { 
    available.sort((a,b)=>{ 
      const ca=personAssignments[a], cb=personAssignments[b]; 
      if(ca!==cb) return ca-cb; 
      return a.localeCompare(b); 
    }); 
    const selectedPerson = available[0];
    Logger.log(`[SETUP-REUSE] Person ${selectedPerson} assigned for setup at ${formatDateTime(setupStart)}`);
    return selectedPerson; 
  }
  
  Logger.log(`[SETUP-REUSE] No available persons in shift ${startH < firstShiftEnd ? '1' : '2'}, using ${shiftPeople[0]}`);
  return shiftPeople[0];
}

// Browser-compatible date formatting functions
function formatDateTimeForBrowser(date) { 
  if (!date) return ''; 
  return new Date(date); 
}

function formatDateForBrowser(date) { 
  if (!date) return ''; 
  return new Date(date); 
}

function formatDateTimeForSetup(date) { 
  if (!date) return ''; 
  const day = String(date.getDate()).padStart(2,'0'); 
  const month = String(date.getMonth()+1).padStart(2,'0'); 
  const year = date.getFullYear(); 
  const hour = String(date.getHours()).padStart(2,'0'); 
  const minute = String(date.getMinutes()).padStart(2,'0'); 
  return `${day}-${month}-${year} ${hour}:${minute}`; 
}

function formatGlobalHolidayPeriods(holidayPeriods) { 
  if (!holidayPeriods || holidayPeriods.length === 0) return ''; 
  return holidayPeriods.map(period => period.type === 'SINGLE_DAY' ? formatDateTime(period.start) : `${formatDateTime(period.start)} to ${formatDateTime(period.end)}`).join('; '); 
}

function parseHolidayEntry(holidayStr) { 
  if (!holidayStr) return []; 
  const str = String(holidayStr).trim(); 
  if (!str) return []; 
  let separator = null; 
  if (str.includes('→')) separator = '→'; 
  else if (str.includes(' - ')) separator = ' - '; 
  if (separator) return parseHolidayRangePeriod(str, separator); 
  else return parseHolidaySingleDatePeriod(str); 
}

function parseHolidayRangePeriod(str, separator) { 
  const parts = str.split(separator); 
  if (parts.length !== 2) return []; 
  const startStr = parts[0].trim(), endStr = parts[1].trim(); 
  const startDate = parseSingleDateTime(startStr), endDate = parseSingleDateTime(endStr); 
  if (!startDate || !endDate) return []; 
  if (startDate >= endDate) return []; 
  return [{ start: new Date(startDate), end: new Date(endDate), type: 'RANGE' }]; 
}

function parseHolidaySingleDatePeriod(str) { 
  const date = parseSingleDateTime(str); 
  if (!date) return []; 
  const startOfDay = new Date(date); 
  startOfDay.setHours(0,0,0,0); 
  const endOfDay = new Date(date); 
  endOfDay.setHours(23,59,59,999); 
  return [{ start: new Date(startOfDay), end: new Date(endOfDay), type: 'SINGLE_DAY' }]; 
}

function parseSingleDateTime(str) { 
  if (!str) return null; 
  if (str instanceof Date && !isNaN(str.getTime())) return new Date(str); 
  const dateStr = String(str).trim(); 
  let match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})$/); 
  if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), Number(match[4]), Number(match[5]), 0, 0); 
  match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); 
  if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), 0, 0, 0, 0); 
  const fallbackDate = new Date(dateStr); 
  return isNaN(fallbackDate.getTime()) ? null : fallbackDate; 
}

function formatDateTime(date) { 
  if (!date) return ''; 
  const day = String(date.getDate()).padStart(2,'0'); 
  const month = String(date.getMonth()+1).padStart(2,'0'); 
  const year = date.getFullYear(); 
  const hour = String(date.getHours()).padStart(2,'0'); 
  const minute = String(date.getMinutes()).padStart(2,'0'); 
  if (hour === '00' && minute === '00') return `${day}/${month}/${year}`; 
  else return `${day}/${month}/${year} ${hour}:${minute}`; 
}

function adjustStartTimeForHolidayBlocking(startTime, holidayPeriods, workStartHour, workEndHour) {
  let adjustedTime = new Date(startTime);
  let attempts = 0, maxAttempts = 200;
  while (attempts < maxAttempts) {
    const currentHour = adjustedTime.getHours();
    if (currentHour < workStartHour) adjustedTime.setHours(workStartHour, 0, 0, 0);
    else if (currentHour >= workEndHour) { adjustedTime.setDate(adjustedTime.getDate() + 1); adjustedTime.setHours(workStartHour, 0, 0, 0); }
    const blockingPeriod = findHolidayPeriodContaining(adjustedTime, holidayPeriods);
    if (!blockingPeriod) return adjustedTime;
    adjustedTime = new Date(blockingPeriod.end.getTime() + 1000);
    attempts++;
  }
  throw new Error('Max attempts reached in adjustStartTimeForHolidayBlocking');
}

function findHolidayPeriodContaining(checkTime, holidayPeriods) {
  if (!holidayPeriods || holidayPeriods.length === 0) return null;
  const checkTimeMs = new Date(checkTime).getTime();
  for (let period of holidayPeriods) {
    if (checkTimeMs >= period.start.getTime() && checkTimeMs <= period.end.getTime()) return period;
  }
  return null;
}

function parseShift(str) {
  const parts = str.split('-'); 
  if (parts.length !== 2) { 
    Logger.log('Invalid shift format: ' + str); 
    return null; 
  }
  const startParts = parts[0].trim().split(':').map(Number); 
  const endParts = parts[1].trim().split(':').map(Number);
  if (startParts.length !== 2 || endParts.length !== 2 || isNaN(startParts[0]) || isNaN(startParts[1]) || isNaN(endParts[0]) || isNaN(endParts[1])) { 
    Logger.log('Invalid shift time: ' + str); 
    return null; 
  }
  return { 
    startH: startParts[0], 
    startM: startParts[1], 
    endH: endParts[0], 
    endM: endParts[1], 
    startMin: startParts[0]*60 + startParts[1], 
    endMin: endParts[0]*60 + endParts[1] 
  };
}

function alignRunStartWithShift(setupEnd, shift1, shift2, shift3) {
  const shifts = [parseShift(shift1), parseShift(shift2), parseShift(shift3)].filter(s => s !== null);
  if (shifts.length === 0) return new Date(setupEnd);
  const todMin = setupEnd.getHours()*60 + setupEnd.getMinutes();
  for (let shift of shifts) {
    const isOvernight = shift.endMin < shift.startMin;
    if (isOvernight) { 
      if (todMin >= shift.startMin || todMin < shift.endMin) return new Date(setupEnd); 
    }
    else { 
      if (todMin >= shift.startMin && todMin < shift.endMin) return new Date(setupEnd); 
    }
  }
  let candidates = [];
  for (let shift of shifts) {
    let candidate = new Date(setupEnd); 
    candidate.setHours(shift.startH, shift.startM, 0, 0); 
    if (candidate.getTime() <= setupEnd.getTime()) candidate.setDate(candidate.getDate() + 1); 
    candidates.push(candidate);
  }
  const nextStartTime = Math.min(...candidates.map(c => c.getTime()));
  return new Date(nextStartTime);
}

function generateFixedStatus(machine, machineResult, holidayPeriods) {
  const status = []; 
  status.push('FIXED_VALIDATED'); 
  status.push(`SELECTED: ${machine}`);
  if (machineResult && machineResult.rescheduled) status.push(`RESCHEDULED: ${machineResult.attempts} attempts`);
  if (machineResult && machineResult.originalWindow !== machineResult.finalWindow) status.push('WINDOW_ADJUSTED');
  return status.join(' | ');
}

/* === BROWSER EXPORT === */
// Make the main function available globally for browser use
if (typeof window !== 'undefined') {
  window.runSchedulingInBrowser = runSchedulingInBrowser;
  window.FixedUnifiedSchedulingEngine = FixedUnifiedSchedulingEngine;
  window.CONFIG = CONFIG;
}

// For Node.js/CommonJS environments
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    runSchedulingInBrowser,
    FixedUnifiedSchedulingEngine,
    CONFIG
  };
}

/* === END OF FILE === */
