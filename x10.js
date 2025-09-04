/**
 * x10.js - Fixed: added countOverlaps + formatClean
 * Last update: 2025-08-28 (patched)
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

/* === FixedUnifiedSchedulingEngine (unchanged except logging) === */
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

/* === MAIN runScheduling (uses helpers below) === */
function runScheduling() {
  const startTime = new Date();
  Logger.log('=== FIXED UNIFIED SCHEDULING ENGINE STARTED ===');
  const fixedEngine = new FixedUnifiedSchedulingEngine();

  try {
    const checkTimeout = () => {
      const elapsed = new Date().getTime() - startTime.getTime();
      if (elapsed > CONFIG.MAX_PROCESSING_TIME_MS) throw new Error('Processing timeout - operation too complex');
    };

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const dbSh = ss.getSheetByName('Database');
    const inSh = ss.getSheetByName('Input');
    if (!dbSh || !inSh) throw new Error("Missing sheets. Ensure 'Database' and 'Input' exist.");

    let outSh = getOrCreateSheet(ss, 'Output');
    let out2Sh = getOrCreateSheet(ss, 'Output_2');
    let setupOutSh = getOrCreateSheet(ss, 'Setup_output');
    let fixedReportSh = getOrCreateSheet(ss, 'Fixed_Report');

    const db = readDatabase(dbSh);
    const inputData = readInput(inSh, db, fixedEngine);
    if (!inputData || !inputData.orders || inputData.orders.length === 0) throw new Error('No valid orders found in input data');
    const { orders, globalStart, brokenMachines, globalHolidayPeriods, setupStartHour, setupEndHour, shift1, shift2, shift3 } = inputData;

    Logger.log(`Processing ${orders.length} orders with ${globalHolidayPeriods.length} holiday periods`);

    const machineCal = initializeMachineCalendar(db, globalStart);
    const setupIntervals = [];
    const personBusy = { 'A': [], 'B': [], 'C': [], 'D': [] };
    const personAssignments = { 'A': 0, 'B': 0, 'C': 0, 'D': 0 };
    const rows = [], rows2 = [], setupRows = [];
    let globalBatchCounter = 1;
    let totalEffectiveMin = 0, totalHolidayPausedMin = 0, totalGapPausedMin = 0;

    // early gap calculation
    const shift3EndStr = shift3.split('-')[1].trim();
    const [shift3EH, shift3EM] = shift3EndStr.split(':').map(Number);
    const shift3EndMin = shift3EH * 60 + shift3EM;
    const shift1StartStr = shift1.split('-')[0].trim();
    const [shift1SH, shift1SM] = shift1StartStr.split(':').map(Number);
    const shift1StartMin = shift1SH * 60 + shift1SM;
    let earlyGapMin = shift1StartMin - shift3EndMin; if (earlyGapMin < 0) earlyGapMin = 0;
    Logger.log(`Early Gap: ${earlyGapMin} minutes`);

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
                order.dueDate ? formatDateForSheet(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', 'NO_ELIGIBLE_MACHINE'
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

              // ===== Enforcement: SetupStart must not be earlier than previous operation's FIRST-PIECE completion
              const prevFirstPieceDone = (prevPieceRunEnds && prevPieceRunEnds.length > 0) ? new Date(prevPieceRunEnds[0]) : null;
              const machineAvailableFromCalendar = machineCal[selectedMachine] || new Date(globalStart);
              const setupDurationMs = setupMin * 60 * 1000;

              const earliestStartMs = Math.max(validatedStart.getTime(), prevFirstPieceDone ? prevFirstPieceDone.getTime() : validatedStart.getTime());
              const syncStartMs = machineAvailableFromCalendar.getTime() - setupDurationMs;
              let setupStartMs = Math.max(earliestStartMs, syncStartMs);

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
                chosenPerson = assignSetupPerson(finalSetupStart, finalSetupEnd, personBusy, personAssignments, setupStartHour, setupEndHour);
                if (chosenPerson && chosenPerson !== 'OP1') { personAssignments[chosenPerson]++; personBusy[chosenPerson].push({ start: new Date(finalSetupStart), end: new Date(finalSetupEnd) }); }
              }

              if (setupMin > 0) setupIntervals.push({ start: new Date(finalSetupStart), end: new Date(finalSetupEnd) });

              machineCal[chosenMachine] = new Date(finalRunEndOverall);
              nextOpEarliest = new Date(finalFirstPieceDone);

              // TIMING (rules from user)
              const timingStr = formatTimingForOperation(finalSetupStart, finalRunStartOverall, finalRunEndOverall, globalHolidayPeriods, shift1, shift2, shift3);

              const holidayPauseMs = (globalHolidayPeriods && globalHolidayPeriods.length > 0) ? computeHolidayOverlapMs(finalRunStartOverall, finalRunEndOverall, globalHolidayPeriods) : 0;
              const shiftGapMs = computeShiftGapMs(finalRunStartOverall, finalRunEndOverall, [shift1, shift2, shift3]);
              totalHolidayPausedMin += Math.round(holidayPauseMs / 60000);
              totalGapPausedMin += Math.round(shiftGapMs / 60000);
              totalEffectiveMin += Math.round((finalRunEndOverall.getTime() - finalSetupStart.getTime()) / 60000);

              rows.push([
                order.partNumber, order.orderQty, order.priority, batchId, batchQty, Number(op.OperationSeq), op.OperationName || '',
                chosenMachine, (chosenPerson === 'OP1') ? '' : chosenPerson, formatDateTimeForSheet(finalSetupStart), formatDateTimeForSheet(finalSetupEnd), formatDateTimeForSheet(finalRunStartOverall), formatDateTimeForSheet(finalRunEndOverall),
                timingStr, order.dueDate ? formatDateForSheet(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', opMachineStatus
              ]);

              rows2.push([order.partNumber, order.orderQty, batchQty, formatDateTimeForSheet(finalRunStartOverall), chosenMachine, formatDateTimeForSheet(finalRunEndOverall)]);

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
                order.dueDate ? formatDateForSheet(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', `SCHEDULING_FAILED: ${schedulingError || 'unknown'}`
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
              order.dueDate ? formatDateForSheet(order.dueDate) : '', brokenMachines.join(', '), formatGlobalHolidayPeriods(globalHolidayPeriods), op.Operator || '', `ERROR: ${opError.toString()}`
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

    saveOutputData(outSh, rows, totalTiming);
    saveOutput2Data(out2Sh, rows2);
    saveSetupOutputData(setupOutSh, setupRows);
    saveFixedReport(fixedReportSh, fixedEngine);
    let clientOutSh = getOrCreateSheet(ss, 'Client_Out');
    generateClientOut(clientOutSh, rows, fixedEngine);

    let dashboardSh = getOrCreateSheet(ss, 'Dashboard');
    const outputData = readOutputData(outSh);
    let currentRow = 1;
    currentRow = generateKPISummary(dashboardSh, outputData, currentRow);
    currentRow = generatePersonnelUtilization(dashboardSh, outputData, currentRow);
    currentRow = generateMachineUtilization(dashboardSh, outputData, currentRow);
    currentRow = generateShiftBasedReport(dashboardSh, outputData, currentRow);
    currentRow = generateExceptionReport(dashboardSh, outputData, currentRow);
    currentRow = generateWeeklyRollUp(dashboardSh, outputData, currentRow);
    formatDashboard(dashboardSh);

    const endTime = new Date();
    const duration = (endTime - startTime) / 1000;
    const fixedReport = fixedEngine.getFixedReport();
    Logger.log(`=== FIXED SCHEDULING COMPLETED === Duration: ${duration}s`);
    logFixedCompletion(fixedReport, duration, rows.length, setupRows.length);

    return { success: true, duration: duration, recordsProcessed: rows.length, setupRecordsProcessed: setupRows.length, fixedReport: fixedReport, message: 'Scheduling completed with piece-level enforcement and timing fixes' };

  } catch (error) {
    Logger.log('ERROR in runScheduling: ' + error.toString());
    const ui = SpreadsheetApp.getUi();
    ui.alert('Scheduling Error', 'Scheduling failed: ' + error.message + '\n\nPlease check:\n- Input data format\n- Database completeness\n- Machine breakdown date formats', ui.ButtonSet.OK);
    throw error;
  }
}

/* === TIMING HELPERS (new & fixed) === */

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

/** formatTimingForOperation per user rules
* Timing = "<elapsed> total (<worked> work; <paused> paused)"
* - Elapsed = RunEnd - SetupStart
* - Worked = minutes inside production shifts between RunStart and RunEnd, excluding holidays
* - Paused = Elapsed - Worked (covers off-shift, holidays, breakdown waiting, setup→run gap)
*/
function formatTimingForOperation(setupStart, runStart, runEnd, holidayPeriods, shift1, shift2, shift3) {
if (!setupStart || !runEnd) return '';
const totalMs = new Date(runEnd).getTime() - new Date(setupStart).getTime();
const workedMs = computeWorkedWithinShiftsExcludingHolidays(setupStart, runEnd, [shift1, shift2, shift3], holidayPeriods); // FIXED: Changed from runStart to setupStart for full wall clock inclusion
const pausedMs = Math.max(0, totalMs - workedMs);

const totalStr = formatDurationMs(totalMs);
const workedStr = formatDurationMs(workedMs);
const pausedStr = formatDurationMs(pausedMs);
return `${totalStr} total (${workedStr} work; ${pausedStr} paused)`;
}

// Compute minutes inside configured production shifts between runStart and runEnd, excluding holidays
function computeWorkedWithinShiftsExcludingHolidays(runStart, runEnd, shiftsArray, holidayPeriods) {
const aStart = new Date(runStart);
const aEnd = new Date(runEnd);
if (!(aStart instanceof Date) || isNaN(aStart) || !(aEnd instanceof Date) || isNaN(aEnd) || aEnd <= aStart) return 0;
const shifts = (shiftsArray || []).filter(Boolean).map(s => parseShift(s)).filter(Boolean);
if (shifts.length === 0) {
  // No shifts -> no allowed work window
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

/* === I/O helpers & remaining functions (kept from previous working version) === */

function getOrCreateSheet(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  sheet.clear();
  return sheet;
}

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

function saveOutputData(sheet, rows, totalTiming) {
  if (!rows || rows.length === 0) return;
  const headers = ["PartNumber", "Order_Quantity", "Priority", "Batch_ID", "Batch_Qty", "OperationSeq", "OperationName", "Machine", "Person", "SetupStart", "SetupEnd", "RunStart", "RunEnd", "Timing", "DueDate", "BreakdownMachine", "Global_Holiday_Periods", "Operator", "Machine_Availability_STATUS"];
  try {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    const chunkSize = 100;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      sheet.getRange(i + 2, 1, chunk.length, chunk[0].length).setValues(chunk);
    }
    sheet.getRange(2, 2, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 5, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 10, rows.length, 4).setNumberFormat('yyyy-mm-dd hh:mm');
    sheet.getRange(2, 15, rows.length, 1).setNumberFormat('yyyy-mm-dd');
    sheet.autoResizeColumns(1, headers.length);
    sheet.getRange(rows.length + 2, 1).setValue('TOTAL (Timing)');
    sheet.getRange(rows.length + 2, 14).setValue(totalTiming);
    sheet.getRange(rows.length + 2, 1, 1, 14).setFontWeight('bold');
  } catch (error) {
    Logger.log('Error saving main output: ' + error.toString());
    throw new Error('Failed to save main output data');
  }
}

function saveOutput2Data(sheet, rows) {
  if (!rows || rows.length === 0) return;
  const headers = ["Part Number", "Quantity", "Batch Size", "Date & Time", "Machine", "Expected Delivery Date"];
  try {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(2, 2, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 3, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 4, rows.length, 1).setNumberFormat('yyyy-mm-dd hh:mm');
    sheet.getRange(2, 6, rows.length, 1).setNumberFormat('yyyy-mm-dd hh:mm');
    sheet.autoResizeColumns(1, headers.length);
  } catch (error) {
    Logger.log('Error saving output 2: ' + error.toString());
    throw new Error('Failed to save secondary output data');
  }
}

function saveSetupOutputData(sheet, rows) {
  if (!rows || rows.length === 0) return;
  const headers = ["PartNumber", "Order_Quantity", "Batch_Qty", "OperationSeq", "Machine", "Person", "SetupStart", "SetupEnd", "Timing"];
  try {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(2, 2, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 3, rows.length, 1).setNumberFormat('0');
    sheet.getRange(2, 4, rows.length, 1).setNumberFormat('0');
    sheet.autoResizeColumns(1, headers.length);
    Logger.log(`Setup output saved: ${rows.length} records`);
  } catch (error) {
    Logger.log('Error saving setup output: ' + error.toString());
    throw new Error('Failed to save setup output data');
  }
}

/* === Remaining helpers (parsing, shifts, report, etc) === */

function readInput(sh, db, fixedEngine) {
  try {
    const vals = sh.getDataRange().getValues();
    if (!vals || vals.length < 2) return { orders: [], globalStart: new Date(), brokenMachines: [], globalHolidayPeriods: [], setupStartHour: 6, setupEndHour: 22, shift1: "06:00-14:00", shift2: "14:00-22:00", shift3: "22:00-06:00" };
    const headers = vals[0].map(h => String(h || '').trim().toLowerCase());
    const findColumnIndex = (targetColumn) => {
      const target = targetColumn.toLowerCase();
      let idx = headers.indexOf(target);
      if (idx !== -1) return idx;
      const columnMappings = { 'partnumber': ['part_number', 'part number', 'pn'], 'operationseq': ['operation_seq', 'operation seq'], 'order_quantity': ['orderquantity', 'order quantity', 'qty'], 'priority': ['prio'], 'duedate': ['due_date', 'due date'], 'breakdownmachine': ['breakdown_machine', 'breakdown machine'], 'breakdown_machines_date_time': ['breakdown_machines_datetime', 'breakdown machines date time'], 'startdatetime': ['start_datetime', 'start date time'], 'holiday': ['holidays'], 'setup_availability_window': ['setup availability window'], 'shift_1': ['shift_1', 'shift1'], 'shift_2': ['shift_2', 'shift2'], 'shift_3': ['shift_3', 'shift3'] };
      if (columnMappings[target]) for (let variant of columnMappings[target]) { idx = headers.indexOf(variant); if (idx !== -1) return idx; }
      return -1;
    };

    const list = []; let globalStart = null; const brokenSet = new Set(); const globalHolidayPeriodsArray = []; let setupStartHour = 6; let setupEndHour = 22; const breakdownData = []; let shift1 = "06:00-14:00"; let shift2 = "14:00-22:00"; let shift3 = "22:00-06:00";

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

    for (let r = 1; r < vals.length; r++) {
      const row = vals[r];
      const pn = String(row[findColumnIndex('partnumber')] || '').trim(); if (!pn) continue;
      const qty = Number(row[findColumnIndex('order_quantity')] || 0);
      let prRaw = String(row[findColumnIndex('priority')] || '').trim().toLowerCase(); if (!prRaw) prRaw = 'normal';
      const prNum = { 'urgent': 1, 'high': 2, 'medium': 3, 'normal': 4, 'low': 5 }[prRaw] || 99;
      const dueRaw = row[findColumnIndex('duedate')];
      const dueDate = parseDateMaybe(dueRaw);
      const brCell = row[findColumnIndex('breakdownmachine')];
      const brokenFromRow = parseMachines(brCell);
      const breakdownDateTimeCell = row[findColumnIndex('breakdown_machines_date_time')];
      if (brokenFromRow.length > 0) { breakdownData.push({ machines: brokenFromRow, dateTime: breakdownDateTimeCell }); if (!breakdownDateTimeCell) brokenFromRow.forEach(machine => brokenSet.add(machine)); }
      const holCell = row[findColumnIndex('holiday')]; if (holCell) { const holidayPeriods = parseHolidayEntry(holCell); globalHolidayPeriodsArray.push(...holidayPeriods); }
      const startRaw = row[findColumnIndex('startdatetime')]; const startParsed = parseDateMaybe(startRaw); if (startParsed) { if (!globalStart || startParsed < globalStart) globalStart = startParsed; }
      const setupAvailCell = row[findColumnIndex('setup_availability_window')]; if (setupAvailCell) { const str = String(setupAvailCell).trim(); const match = str.match(/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/); if (match) { setupStartHour = Number(match[1]); setupEndHour = Number(match[3]); } }
      const shift1Cell = row[findColumnIndex('shift_1')]; if (shift1Cell) shift1 = String(shift1Cell).trim();
      const shift2Cell = row[findColumnIndex('shift_2')]; if (shift2Cell) shift2 = String(shift2Cell).trim();
      const shift3Cell = row[findColumnIndex('shift_3')]; if (shift3Cell) shift3 = String(shift3Cell).trim();
      if (!db[pn] || !db[pn].length || qty <= 0) continue;
      list.push({ partNumber: pn, orderQty: qty, priority: prRaw, priorityNum: prNum, dueDate: dueDate, onlySeqs: null });
    }

    fixedEngine.createExclusionMatrix(breakdownData);
    if (!globalStart) globalStart = new Date();
    list.sort((a, b) => { const pa = a.priorityNum, pb = b.priorityNum; if (pa !== pb) return pa - pb; const da = a.dueDate ? a.dueDate.getTime() : Number.POSITIVE_INFINITY; const dbv = b.dueDate ? b.dueDate.getTime() : Number.POSITIVE_INFINITY; if (da !== dbv) return da - dbv; return String(a.partNumber).localeCompare(String(b.partNumber)); });
    
    // Sort and merge overlapping/touching holiday periods
    globalHolidayPeriodsArray.sort((a, b) => a.start.getTime() - b.start.getTime());
    const mergedH = [];
    for (const cur of globalHolidayPeriodsArray) {
      if (!mergedH.length) { mergedH.push({ start: new Date(cur.start), end: new Date(cur.end), type: cur.type || 'RANGE' }); continue; }
      const last = mergedH[mergedH.length - 1];
      if (cur.start.getTime() <= last.end.getTime()) {
        if (cur.end.getTime() > last.end.getTime()) last.end = new Date(cur.end);
      } else {
        mergedH.push({ start: new Date(cur.start), end: new Date(cur.end), type: cur.type || 'RANGE' });
      }
    }
    globalHolidayPeriodsArray.length = 0; globalHolidayPeriodsArray.push(...mergedH);
    Logger.log(`[HOLIDAY] Final merged holidays count: ${globalHolidayPeriodsArray.length}`);
    globalHolidayPeriodsArray.forEach((h, i) => {
      Logger.log(`[HOLIDAY] ${i+1}: ${formatDateTime(h.start)} → ${formatDateTime(h.end)}`);
    });

    return { orders: list, globalStart, brokenMachines: Array.from(brokenSet), globalHolidayPeriods: globalHolidayPeriodsArray, setupStartHour, setupEndHour, shift1, shift2, shift3 };
  } catch (error) {
    Logger.log('Error reading input: ' + error.toString());
    throw new Error('Failed to read input sheet');
  }
}

function readDatabase(sh) {
  try {
    const vals = sh.getDataRange().getValues();
    if (!vals || vals.length < 2) return {};
    const headers = vals[0].map(h => String(h || '').trim().toLowerCase());
    const idx = (name) => headers.indexOf(name.toLowerCase());
    const db = {};
    for (let r = 1; r < vals.length; r++) {
      const row = vals[r];
      const pn = String(row[idx('partnumber')] || '').trim(); if (!pn) continue;
      const eligible = parseMachines(row[idx('eligiblemachines')]);
      const op = { PartNumber: pn, OperationSeq: Number(row[idx('operationseq')] || 0), OperationName: String(row[idx('operationname')] || '').trim(), SetupTime_Min: Number(row[idx('setuptime_min')] || 0), CycleTime_Min: Number(row[idx('cycletime_min')] || 0), Minimum_BatchSize: Number(row[idx('minimum_batchsize')] || 1), EligibleMachines: eligible, Operator: row[idx('operator')] ? Number(row[idx('operator')]) : null };
      if (!db[pn]) db[pn] = []; db[pn].push(op);
    }
    return db;
  } catch (error) { Logger.log('Error reading database: ' + error.toString()); throw new Error('Failed to read database sheet'); }
}

/* --- utility parsing / formatting functions reused --- */

function parseMachines(cell) { if (cell === null || cell === undefined) return []; const s = String(cell).trim(); if (s === '') return []; return s.split(',').map(x => x.trim()).filter(Boolean); }
function parseDateMaybe(cell) { if (!cell && cell !== 0) return null; if (cell instanceof Date && !isNaN(cell.getTime())) return new Date(cell); const s = String(cell).trim(); if (!s) return null; let m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?$/); if (m) { const yyyy=Number(m[1]), MM=Number(m[2])-1, dd=Number(m[3]); const hh=m[4]?Number(m[4]):0; const mi=m[5]?Number(m[5]):0; const d=new Date(yyyy,MM,dd,hh,mi,0,0); if(!isNaN(d.getTime())) return d; } m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T](\d{1,2}):(\d{2}))?$/); if (m) { const dd=Number(m[1]), MM=Number(m[2])-1, yyyy=Number(m[3]); const hh=m[4]?Number(m[4]):0; const mi=m[5]?Number(m[5]):0; const d=new Date(yyyy,MM,dd,hh,mi,0,0); if(!isNaN(d.getTime())) return d; } const d2 = new Date(s); return isNaN(d2.getTime()) ? null : d2; }

function makeBatches(total, minBatch) { total = Math.max(0, Number(total || 0)); minBatch = Math.max(1, Number(minBatch || 1)); if (total === 0) return []; if (total <= minBatch) return [total]; const half = Math.floor(total / 2); if (half >= minBatch) return [half, total - half]; return [total]; }

/* === NEW: countOverlaps helper ===
   Counts how many intervals in busyArray overlap [start, end).
   busyArray expected to be array of {start: Date, end: Date}
*/
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

/* === NEW: formatClean helper ===
   Convert total minutes to compact readable string (e.g. "1D 3H 5M", "3H", "45M").
   Input: totalMinutes (number)
*/
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

function assignSetupPerson(setupStart, setupEnd, personBusy, personAssignments, setupStartHour, setupEndHour) {
  const setupStartDate = new Date(setupStart); const startH = setupStartDate.getHours();
  const shiftLength = (setupEndHour - setupStartHour) / 2; const firstShiftEnd = setupStartHour + shiftLength;
  const shiftPeople = startH < firstShiftEnd ? ['A','B'] : ['C','D'];
  const available = shiftPeople.filter(p => countOverlaps(setupStartDate, new Date(setupEnd), personBusy[p]) === 0);
  if (available.length > 0) { available.sort((a,b)=>{ const ca=personAssignments[a], cb=personAssignments[b]; if(ca!==cb) return ca-cb; return a.localeCompare(b); }); return available[0]; }
  return shiftPeople[0];
}

function formatDateTimeForSheet(date) { if (!date) return ''; return new Date(date); }
function formatDateForSheet(date) { if (!date) return ''; return new Date(date); }
function formatDateTimeForSetup(date) { if (!date) return ''; const day = String(date.getDate()).padStart(2,'0'); const month = String(date.getMonth()+1).padStart(2,'0'); const year = date.getFullYear(); const hour = String(date.getHours()).padStart(2,'0'); const minute = String(date.getMinutes()).padStart(2,'0'); return `${day}-${month}-${year} ${hour}:${minute}`; }
function formatGlobalHolidayPeriods(holidayPeriods) { if (!holidayPeriods || holidayPeriods.length === 0) return ''; return holidayPeriods.map(period => period.type === 'SINGLE_DAY' ? formatDateTime(period.start) : `${formatDateTime(period.start)} to ${formatDateTime(period.end)}`).join('; '); }

function parseHolidayEntry(holidayStr) { if (!holidayStr) return []; const str = String(holidayStr).trim(); if (!str) return []; let separator = null; if (str.includes('→')) separator = '→'; else if (str.includes(' - ')) separator = ' - '; if (separator) return parseHolidayRangePeriod(str, separator); else return parseHolidaySingleDatePeriod(str); }
function parseHolidayRangePeriod(str, separator) { const parts = str.split(separator); if (parts.length !== 2) return []; const startStr = parts[0].trim(), endStr = parts[1].trim(); const startDate = parseSingleDateTime(startStr), endDate = parseSingleDateTime(endStr); if (!startDate || !endDate) return []; if (startDate >= endDate) return []; return [{ start: new Date(startDate), end: new Date(endDate), type: 'RANGE' }]; }
function parseHolidaySingleDatePeriod(str) { const date = parseSingleDateTime(str); if (!date) return []; const startOfDay = new Date(date); startOfDay.setHours(0,0,0,0); const endOfDay = new Date(date); endOfDay.setHours(23,59,59,999); return [{ start: new Date(startOfDay), end: new Date(endOfDay), type: 'SINGLE_DAY' }]; }
function parseSingleDateTime(str) { if (!str) return null; if (str instanceof Date && !isNaN(str.getTime())) return new Date(str); const dateStr = String(str).trim(); let match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{2})$/); if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), Number(match[4]), Number(match[5]), 0, 0); match = dateStr.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/); if (match) return new Date(Number(match[3]), Number(match[2]) - 1, Number(match[1]), 0, 0, 0, 0); const fallbackDate = new Date(dateStr); return isNaN(fallbackDate.getTime()) ? null : fallbackDate; }

function formatDateTime(date) { if (!date) return ''; const day = String(date.getDate()).padStart(2,'0'); const month = String(date.getMonth()+1).padStart(2,'0'); const year = date.getFullYear(); const hour = String(date.getHours()).padStart(2,'0'); const minute = String(date.getMinutes()).padStart(2,'0'); if (hour === '00' && minute === '00') return `${day}/${month}/${year}`; else return `${day}/${month}/${year} ${hour}:${minute}`; }

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
  const parts = str.split('-'); if (parts.length !== 2) { Logger.log('Invalid shift format: ' + str); return null; }
  const startParts = parts[0].trim().split(':').map(Number); const endParts = parts[1].trim().split(':').map(Number);
  if (startParts.length !== 2 || endParts.length !== 2 || isNaN(startParts[0]) || isNaN(startParts[1]) || isNaN(endParts[0]) || isNaN(endParts[1])) { Logger.log('Invalid shift time: ' + str); return null; }
  return { startH: startParts[0], startM: startParts[1], endH: endParts[0], endM: endParts[1], startMin: startParts[0]*60 + startParts[1], endMin: endParts[0]*60 + endParts[1] };
}

function alignRunStartWithShift(setupEnd, shift1, shift2, shift3) {
  const shifts = [parseShift(shift1), parseShift(shift2), parseShift(shift3)].filter(s => s !== null);
  if (shifts.length === 0) return new Date(setupEnd);
  const todMin = setupEnd.getHours()*60 + setupEnd.getMinutes();
  for (let shift of shifts) {
    const isOvernight = shift.endMin < shift.startMin;
    if (isOvernight) { if (todMin >= shift.startMin || todMin < shift.endMin) return new Date(setupEnd); }
    else { if (todMin >= shift.startMin && todMin < shift.endMin) return new Date(setupEnd); }
  }
  let candidates = [];
  for (let shift of shifts) {
    let candidate = new Date(setupEnd); candidate.setHours(shift.startH, shift.startM, 0, 0); if (candidate.getTime() <= setupEnd.getTime()) candidate.setDate(candidate.getDate() + 1); candidates.push(candidate);
  }
  const nextStartTime = Math.min(...candidates.map(c => c.getTime()));
  return new Date(nextStartTime);
}

function generateFixedStatus(machine, machineResult, holidayPeriods) {
  const status = []; status.push('FIXED_VALIDATED'); status.push(`SELECTED: ${machine}`);
  if (machineResult && machineResult.rescheduled) status.push(`RESCHEDULED: ${machineResult.attempts} attempts`);
  if (machineResult && machineResult.originalWindow !== machineResult.finalWindow) status.push('WINDOW_ADJUSTED');
  return status.join(' | ');
}

/* --- reporting + client out (unchanged) --- */

function saveFixedReport(sheet, fixedEngine) {
  try {
    sheet.clear();
    const report = fixedEngine.getFixedReport(); const timestamp = new Date();
    const allData = [ ['FIXED UNIFIED SCHEDULING ENGINE REPORT', '', '', '', '', '', '', ''], [`Generated: ${fixedEngine.formatDateTime(timestamp)}`, '', '', '', '', '', '', ''], [`Exclusion Rules: ${report.exclusionRulesCount}`, '', '', '', '', '', '', ''], [`Total Assignments: ${report.totalAssignments}`, '', '', '', '', '', '', ''], [`Total Rejections: ${report.totalRejections}`, '', '', '', '', '', '', ''], [`Total Reschedules: ${report.totalReschedules}`, '', '', '', '', '', '', ''], [`Validation Failures: ${report.totalValidationFailures}`, '', '', '', '', '', '', ''], ['', '', '', '', '', '', '', ''], ['EXCLUSION MATRIX', '', '', '', '', '', '', ''], ['Machine', 'Rules Count', 'Rule Types', 'Descriptions', '', '', '', ''] ];
    if (report.exclusionMatrix.length > 0) report.exclusionMatrix.forEach(entry => { const ruleTypes = entry.rules.map(r => r.type).join(', '); const descriptions = entry.rules.map(r => r.description).join('; '); allData.push([entry.machine, entry.rulesCount, ruleTypes, descriptions, '', '', '', '']); });
    allData.push(['', '', '', '', '', '', '', '']); allData.push(['SUCCESSFUL ASSIGNMENTS', '', '', '', '', '', '', '']); allData.push(['Machine', 'Operation', 'Final Window', 'Original Window', 'Attempts', 'Rescheduled', '', '']);
    if (report.assignments.length > 0) report.assignments.forEach(assignment => allData.push([ assignment.selectedMachine, `${assignment.operation.partNumber} Op${assignment.operation.operationSeq}`, assignment.finalWindow, assignment.originalWindow, assignment.attempts, assignment.attempts > 1 ? 'Yes' : 'No', '', '' ]));
    if (report.reschedules.length > 0) { allData.push(['', '', '', '', '', '', '', '']); allData.push(['RESCHEDULES', '', '', '', '', '', '', '']); allData.push(['Operation', 'Original Window', 'Rescheduled Window', 'Reason', 'Attempt', '', '', '']); report.reschedules.forEach(reschedule => allData.push([ `${reschedule.operation.partNumber} Op${reschedule.operation.operationSeq}`, reschedule.originalWindow, reschedule.rescheduledWindow, reschedule.reason, reschedule.attempt, '', '', '' ])); }
    const numRows = allData.length; const numCols = 8; if (numRows > 0 && numCols > 0) { sheet.getRange(1,1,numRows,numCols).setValues(allData); sheet.getRange(1,1).setBackground('#1f4e79').setFontColor('white').setFontWeight('bold').setFontSize(14); sheet.autoResizeColumns(1,numCols); }
    Logger.log(`Fixed report saved: ${report.totalAssignments} assignments, ${report.totalReschedules} reschedules`);
  } catch (error) { Logger.log('Error saving fixed report: ' + error.toString()); }
}

function logFixedCompletion(fixedReport, duration, mainRows, setupRows) {
  try {
    Logger.log(`=== FIXED SCHEDULING COMPLETED ===`);
    Logger.log(`Duration: ${duration.toFixed(1)} seconds`);
    Logger.log(`Main operations: ${mainRows}`); Logger.log(`Setup operations: ${setupRows}`);
    Logger.log(`Exclusion rules applied: ${fixedReport.exclusionRulesCount}`); Logger.log(`Successful assignments: ${fixedReport.totalAssignments}`);
    Logger.log(`Machine rejections: ${fixedReport.totalRejections}`); Logger.log(`Reschedules performed: ${fixedReport.totalReschedules}`); Logger.log(`Validation failures: ${fixedReport.totalValidationFailures}`);
    if (fixedReport.totalValidationFailures === 0) Logger.log(`✅ SUCCESS: Zero validation failures - fixed engine working perfectly`);
    if (fixedReport.totalReschedules > 0) Logger.log(`⚠️ ${fixedReport.totalReschedules} operations rescheduled to avoid breakdown conflicts`);
  } catch (error) { Logger.log('Could not log fixed completion: ' + error.toString()); }
}

function generateClientOut(sheet, rows, fixedEngine) {
  const partMap = new Map(); const partsOrder = [];
  rows.forEach(row => {
    const pn = row[0], machine = row[7], setupStart = row[9], runEnd = row[12], timingStr = row[13];
    if (machine === 'NO AVAILABLE MACHINE' || machine === 'NO ELIGIBLE MACHINE' || !setupStart || !runEnd) return;
    if (!partMap.has(pn)) { partsOrder.push(pn); partMap.set(pn, { orderQty: row[1], minStart: setupStart, maxEnd: runEnd, sumHoliday: 0, sumGap: 0, sumBreakdown: 0 }); }
    else { const data = partMap.get(pn); if (setupStart < data.minStart) data.minStart = setupStart; if (runEnd > data.maxEnd) data.maxEnd = runEnd; }
    const { holiday, gap } = parsePauses(timingStr); partMap.get(pn).sumHoliday += holiday; partMap.get(pn).sumGap += gap;
  });

  fixedEngine.rescheduleLog.forEach(reschedule => {
    const pn = reschedule.operation.partNumber;
    if (partMap.has(pn)) {
      const origStart = parseWindowStart(reschedule.originalWindow, fixedEngine);
      const newStart = parseWindowStart(reschedule.rescheduledWindow, fixedEngine);
      const delayMs = newStart.getTime() - origStart.getTime(); const delayMin = Math.max(0, delayMs / 60000);
      partMap.get(pn).sumBreakdown += delayMin;
    }
  });

  const clientRows = [];
  partsOrder.forEach(pn => {
    const data = partMap.get(pn); if (!data.minStart || !data.maxEnd) return;
    const spanMs = data.maxEnd.getTime() - data.minStart.getTime(); const spanMin = Math.max(0, spanMs / 60000); const totalStr = formatDurationMs(spanMs);
    const pauses = [];
    if (data.sumGap > 0) pauses.push(`${formatClean(Math.round(data.sumGap))} due to shift gaps`);
    if (data.sumHoliday > 0) pauses.push(`${formatClean(Math.round(data.sumHoliday))} due to holidays`);
    if (data.sumBreakdown > 0) pauses.push(`${formatClean(Math.round(data.sumBreakdown))} due to breakdowns`);
    let pauseStr = ''; if (pauses.length > 0) pauseStr = `(paused ${pauses.join(', ')})`;
    const timingStr = `${totalStr} ${pauseStr}`.trim();
    clientRows.push([ pn, data.orderQty, timingStr, formatDateTimeClient(data.minStart), formatDateTimeClient(data.maxEnd) ]);
  });

  saveClientOutData(sheet, clientRows);
}

function parsePauses(timingStr) {
  let holiday = 0, gap = 0;
  const holidayMatch = timingStr.match(/paused ([\dDHM ]+) during holidays/);
  if (holidayMatch) holiday = parseTimeStr(holidayMatch[1]);
  const gapMatch = timingStr.match(/paused ([\dDHM ]+) due to shift gaps/);
  if (gapMatch) gap = parseTimeStr(gapMatch[1]);
  return { holiday, gap };
}

function parseTimeStr(str) {
  let min = 0;
  const dMatch = str.match(/(\d+)D/); if (dMatch) min += parseInt(dMatch[1]) * 1440;
  const hMatch = str.match(/(\d+)H/); if (hMatch) min += parseInt(hMatch[1]) * 60;
  const mMatch = str.match(/(\d+)M/); if (mMatch) min += parseInt(mMatch[1]);
  return min;
}

function parseWindowStart(windowStr, fixedEngine) {
  const parts = windowStr.split(' to '); if (parts.length !== 2) return new Date(0); return fixedEngine.parseSingleDateTime(parts[0]);
}

function formatDateTimeClient(date) {
  if (!(date instanceof Date) || isNaN(date.getTime())) return '';
  const yyyy = date.getFullYear(), mm = String(date.getMonth()+1).padStart(2,'0'), dd = String(date.getDate()).padStart(2,'0'), hh = String(date.getHours()).padStart(2,'0'), mi = String(date.getMinutes()).padStart(2,'0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}`;
}

function saveClientOutData(sheet, rows) {
  if (!rows || rows.length === 0) return;
  const headers = ["PartNumber", "Order_Quantity", "Timing", "Start Date", "Expected Delivery Date"];
  try {
    sheet.getRange(1,1,1,headers.length).setValues([headers]);
    if (rows.length > 0) { sheet.getRange(2,1,rows.length,rows[0].length).setValues(rows); sheet.autoResizeColumns(1,headers.length); }
    Logger.log(`Client_Out saved: ${rows.length} records`);
  } catch (error) { Logger.log('Error saving Client_Out: ' + error.toString()); throw new Error('Failed to save Client_Out data'); }
}

/* === END OF FILE === */
// ==================== DASHBOARD FUNCTIONS ====================

function readOutputData(sheet) {
  const vals = sheet.getDataRange().getValues();
  if (!vals || vals.length < 2) return [];
  
  const headers = vals[0].map(h => String(h || '').trim().toLowerCase());
  const getIdx = (name) => headers.indexOf(name.toLowerCase());
  
  const data = [];
  for (let r = 1; r < vals.length; r++) {
    const row = vals[r];
    
    if (String(row[getIdx('machine')] || '').includes('NO ELIGIBLE') || 
        String(row[getIdx('machine')] || '').includes('NO AVAILABLE')) continue;
    
    const item = {
      partNumber: String(row[getIdx('partnumber')] || ''),
      orderQty: Number(row[getIdx('order_quantity')] || 0),
      priority: String(row[getIdx('priority')] || ''),
      batchId: String(row[getIdx('batch_id')] || ''),
      batchQty: Number(row[getIdx('batch_qty')] || 0),
      operationSeq: Number(row[getIdx('operationseq')] || 0),
      operationName: String(row[getIdx('operationname')] || ''),
      machine: String(row[getIdx('machine')] || ''),
      person: String(row[getIdx('person')] || ''),
      setupStart: parseDate(row[getIdx('setupstart')]),
      setupEnd: parseDate(row[getIdx('setupend')]),
      runStart: parseDate(row[getIdx('runstart')]),
      runEnd: parseDate(row[getIdx('runend')]),
      timing: String(row[getIdx('timing')] || ''),
      dueDate: parseDate(row[getIdx('duedate')]),
      operator: row[getIdx('operator')] || ''
    };
    
    if (item.setupStart && item.runEnd) {
      data.push(item);
    }
  }
  
  return data;
}

function generateKPISummary(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("1. KPI SUMMARY");
  sheet.getRange(row, 1).setBackground('#1f4e79').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const totalBatches = new Set(data.map(d => d.batchId)).size;
  const completedOperations = data.length;
  const { delayedBatches } = calculateDelayedBatches(data);
  const totalSetupHours = calculateTotalSetupHours(data);
  const avgSetupDuration = completedOperations > 0 ? totalSetupHours / completedOperations : 0;
  const totalMachineHours = calculateTotalMachineHours(data);
  const weekSpan = calculateWeekSpan(data);
  const plannedHours = calculatePlannedHours(data);
  const machineAnalysis = analyzeMachineUtilization(data, weekSpan);
  const avgMachineUtil = machineAnalysis.machines.length > 0 ? machineAnalysis.machines.reduce((sum, m) => sum + m.utilization, 0) / machineAnalysis.machines.length : 0;
  const scheduleEfficiency = plannedHours > 0 ? totalMachineHours / plannedHours : 0;
  
  const formatValue = (num, type) => {
    if (isNaN(num) || num === 0) {
      if (type === 'h') return '0h';
      if (type === '%') return '0%';
      if (type === 'days') return '0 days';
      return '0';
    }
    if (type === 'h') return num.toFixed(1) + 'h';
    if (type === '%') return num.toFixed(2) + '%';
    if (type === 'days') return num + ' days';
    return num;
  };
  
  const kpiData = [
    ['KPI Metric', 'Value', 'Status', 'Target', 'Performance'],
    ['Total Batches Processed', totalBatches, totalBatches > 0 ? '✅ Active' : '❌ No Data', '> 0', totalBatches > 0 ? 'GOOD' : 'POOR'],
    ['Completed Operations', completedOperations, completedOperations > 0 ? '✅ Complete' : '⏳ Pending', completedOperations, 'ACTIVE'],
    ['Delayed Batches', delayedBatches, delayedBatches === 0 ? '✅ On Time' : '⚠️ Delayed', '0', delayedBatches === 0 ? 'GOOD' : 'POOR'],
    ['Avg Setup Duration', formatValue(avgSetupDuration, 'h'), avgSetupDuration < 2 ? '✅ Efficient' : '⚠️ Long', '< 2h', avgSetupDuration < 2 ? 'GOOD' : 'POOR'],
    ['Total Machine Hours', formatValue(totalMachineHours, 'h'), totalMachineHours > 0 ? '✅ Active' : '❌ Idle', '> 40h/week', totalMachineHours > 40 ? 'GOOD' : 'POOR'],
    ['Total Setup Hours', formatValue(totalSetupHours, 'h'), totalSetupHours > 0 ? '✅ Active' : '❌ Idle', 'N/A', 'N/A'],
    ['Average Machine Utilization', formatValue(avgMachineUtil * 100, '%'), avgMachineUtil > 0 ? '✅ Active' : '❌ Idle', '> 60%', avgMachineUtil > 0.6 ? 'GOOD' : 'POOR'],
    ['Schedule Efficiency', formatValue(scheduleEfficiency * 100, '%'), scheduleEfficiency > 0 ? '✅ Active' : '❌ Idle', 'N/A', 'N/A'],
    ['Week Span', formatValue(weekSpan, 'days'), weekSpan <= 7 ? '✅ On Track' : '⚠️ Extended', '≤ 7 days', weekSpan <= 7 ? 'GOOD' : 'POOR']
  ];
  
  sheet.getRange(row, 1, kpiData.length, kpiData[0].length).setValues(kpiData);
  sheet.getRange(row, 1, 1, kpiData[0].length).setBackground('#d5e4f1').setFontWeight('bold');
  
  return row + kpiData.length + 2;
}

function generatePersonnelUtilization(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("2. PERSONNEL UTILIZATION");
  sheet.getRange(row, 1).setBackground('#70ad47').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const weekSpan = calculateWeekSpan(data);
  const personnelAnalysis = analyzePersonnelUtilization(data, weekSpan);
  
  sheet.getRange(row, 1).setValue("Setup Person Allocation by Shift:");
  sheet.getRange(row, 1).setFontWeight('bold');
  row++;
  
  const setupData = [
    ['Shift', 'Persons Used', 'Total Available', 'Utilization %', 'Status'],
    ...personnelAnalysis.setupByShift.map(shift => [
      shift.name,
      shift.personsUsed,
      shift.totalAvailable,
      shift.utilization > 0 ? (shift.utilization * 100).toFixed(2) + '%' : '0%',
      shift.utilization >= 0.8 ? '🔥 High' : shift.utilization >= 0.4 ? '✅ Normal' : '⚠️ Low'
    ])
  ];
  
  sheet.getRange(row, 1, setupData.length, setupData[0].length).setValues(setupData);
  row += setupData.length + 1;
  
  sheet.getRange(row, 1).setValue("Personnel Efficiency Summary:");
  sheet.getRange(row, 1).setFontWeight('bold');
  row++;
  
  const efficiencyData = [
    ['Person', 'Total Hours', 'Active Hours', 'Idle Hours', 'Utilization %', 'Flag'],
    ...personnelAnalysis.personEfficiency.map(person => [
      person.name || 'Machine Only',
      person.totalHours > 0 ? person.totalHours.toFixed(1) + 'h' : '0h',
      person.activeHours > 0 ? person.activeHours.toFixed(1) + 'h' : '0h',
      person.idleHours > 0 ? person.idleHours.toFixed(1) + 'h' : '0h',
      person.utilization > 0 ? (person.utilization * 100).toFixed(2) + '%' : '0%',
      person.utilization < 0.4 ? '🔴 Underutilized' : person.utilization > 0.9 ? '🔥 Overloaded' : '✅ Normal'
    ])
  ];
  
  sheet.getRange(row, 1, efficiencyData.length, efficiencyData[0].length).setValues(efficiencyData);
  
  return row + efficiencyData.length + 2;
}

function generateMachineUtilization(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("3. MACHINE UTILIZATION");
  sheet.getRange(row, 1).setBackground('#c65911').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const weekSpan = calculateWeekSpan(data);
  const machineAnalysis = analyzeMachineUtilization(data, weekSpan);
  
  sheet.getRange(row, 1).setValue("Machine Performance Summary:");
  sheet.getRange(row, 1).setFontWeight('bold');
  row++;
  
  const machineData = [
    ['Machine', 'Run Hours', 'Setup Hours', 'Total Hours', 'Utilization %', 'Batches', 'Status'],
    ...machineAnalysis.machines.map(machine => [
      machine.name,
      machine.runHours > 0 ? machine.runHours.toFixed(1) + 'h' : '0h',
      machine.setupHours > 0 ? machine.setupHours.toFixed(1) + 'h' : '0h',
      machine.totalHours > 0 ? machine.totalHours.toFixed(1) + 'h' : '0h',
      machine.utilization > 0 ? (machine.utilization * 100).toFixed(2) + '%' : '0%',
      machine.batches,
      machine.utilization >= 0.8 ? '🔥 High Load' : machine.utilization >= 0.4 ? '✅ Normal' : '⚠️ Underused'
    ])
  ];
  
  sheet.getRange(row, 1, machineData.length, machineData[0].length).setValues(machineData);
  row += machineData.length + 1;
  
  sheet.getRange(row, 1).setValue("Bottleneck Analysis (Top 3):");
  sheet.getRange(row, 1).setFontWeight('bold');
  row++;
  
  const bottlenecks = machineAnalysis.machines
    .filter(m => m.utilization > 0.85)
    .sort((a, b) => b.utilization - a.utilization)
    .slice(0, 3);
  
  if (bottlenecks.length > 0) {
    const bottleneckData = [
      ['Rank', 'Machine', 'Utilization %', 'Risk Level', 'Recommendation'],
      ...bottlenecks.map((machine, idx) => [
        idx + 1,
        machine.name,
        (machine.utilization * 100).toFixed(2) + '%',
        machine.utilization > 0.95 ? '🔴 Critical' : '🟡 High',
        machine.utilization > 0.95 ? 'Add capacity' : 'Monitor closely'
      ])
    ];
    sheet.getRange(row, 1, bottleneckData.length, bottleneckData[0].length).setValues(bottleneckData);
    row += bottleneckData.length;
  } else {
    sheet.getRange(row, 1).setValue("✅ No bottlenecks detected - all machines under 85% utilization");
    row++;
  }
  
  return row + 2;
}

function generateShiftBasedReport(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("4. SHIFT-BASED ACTIVITY (3-Hour Windows)");
  sheet.getRange(row, 1).setBackground('#7030a0').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const shiftAnalysis = analyzeShiftActivity(data);
  
  sheet.getRange(row, 1).setValue("Activity Heatmap by Time Window:");
  sheet.getRange(row, 1).setFontWeight('bold');
  row++;
  
  const windows = ['06:00-09:00', '09:00-12:00', '12:00-15:00', '15:00-18:00', '18:00-21:00', '21:00-24:00'];
  const heatmapData = [
    ['Resource Type', ...windows, 'Total Active Hours'],
    ...shiftAnalysis.heatmap.map(resource => [
      resource.name,
      ...resource.windows.map(w => w.activeHours > 0 ? w.activeHours.toFixed(1) + 'h' : '0h'),
      resource.totalHours > 0 ? resource.totalHours.toFixed(1) + 'h' : '0h'
    ])
  ];
  
  sheet.getRange(row, 1, heatmapData.length, heatmapData[0].length).setValues(heatmapData);
  
  return row + heatmapData.length + 2;
}

function generateExceptionReport(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("5. ALERTS");
  sheet.getRange(row, 1).setBackground('#c00000').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const weekSpan = calculateWeekSpan(data);
  const exceptions = findExceptions(data, weekSpan);
  
  if (exceptions.delayed.length > 0) {
    sheet.getRange(row, 1).setValue("🔴 Delayed Batches:");
    sheet.getRange(row, 1).setFontWeight('bold');
    row++;
    
    const delayedData = [
      ['Batch ID', 'Part Number', 'Due Date', 'Completion Date', 'Delay Hours', 'Impact'],
      ...exceptions.delayed.map(item => [
        item.batchId,
        item.partNumber,
        formatDateTime(item.dueDate),
        formatDateTime(item.runEnd),
        item.delayHours > 0 ? item.delayHours.toFixed(1) + 'h' : '0h',
        item.delayHours > 24 ? 'High' : 'Medium'
      ])
    ];
    
    sheet.getRange(row, 1, delayedData.length, delayedData[0].length).setValues(delayedData);
    row += delayedData.length + 1;
  }
  
  if (exceptions.overutilized.length > 0) {
    sheet.getRange(row, 1).setValue("🔥 Overutilized Resources (>90%):");
    sheet.getRange(row, 1).setFontWeight('bold');
    row++;
    
    const overData = [
      ['Resource', 'Type', 'Utilization %', 'Risk Level'],
      ...exceptions.overutilized.map(item => [
        item.name,
        item.type,
        (item.utilization * 100).toFixed(2) + '%',
        item.utilization > 0.95 ? '🔴 Critical' : '🟡 High'
      ])
    ];
    
    sheet.getRange(row, 1, overData.length, overData[0].length).setValues(overData);
    row += overData.length + 1;
  }
  
  if (exceptions.underutilized.length > 0) {
    sheet.getRange(row, 1).setValue("⚠️ Underutilized Resources (<40%):");
    sheet.getRange(row, 1).setFontWeight('bold');
    row++;
    
    const underData = [
      ['Resource', 'Type', 'Utilization %', 'Opportunity'],
      ...exceptions.underutilized.map(item => [
        item.name,
        item.type,
        (item.utilization * 100).toFixed(2) + '%',
        'Capacity Available'
      ])
    ];
    
    sheet.getRange(row, 1, underData.length, underData[0].length).setValues(underData);
    row += underData.length;
  }
  
  if (exceptions.delayed.length === 0 && exceptions.overutilized.length === 0 && exceptions.underutilized.length === 0) {
    sheet.getRange(row, 1).setValue("✅ No exceptions found - all operations within normal parameters");
    row++;
  }
  
  return row + 2;
}

function generateWeeklyRollUp(sheet, data, startRow) {
  let row = startRow;
  
  sheet.getRange(row, 1, 1, 8).merge();
  sheet.getRange(row, 1).setValue("6. WEEKLY ROLL-UP");
  sheet.getRange(row, 1).setBackground('#2f5597').setFontColor('white').setFontWeight('bold');
  row += 2;
  
  const weeklySummary = calculateWeeklySummary(data);
  
  const summaryData = [
    ['Metric', 'Value', 'Performance'],
    ['Total Operations Completed', weeklySummary.totalOperations, '✅'],
    ['Total Machine Hours', weeklySummary.totalMachineHours > 0 ? weeklySummary.totalMachineHours.toFixed(1) + 'h' : '0h', weeklySummary.totalMachineHours > 100 ? '✅' : '⚠️'],
    ['Total Setup Hours', weeklySummary.totalSetupHours > 0 ? weeklySummary.totalSetupHours.toFixed(1) + 'h' : '0h', '📊'],
    ['Average Machine Utilization', (weeklySummary.avgMachineUtil * 100).toFixed(2) + '%', weeklySummary.avgMachineUtil > 0.6 ? '✅' : '⚠️'],
    ['Average Person Utilization', (weeklySummary.avgPersonUtil * 100).toFixed(2) + '%', weeklySummary.avgPersonUtil > 0.5 ? '✅' : '⚠️'],
    ['On-Time Completion Rate', (weeklySummary.onTimeRate * 100).toFixed(2) + '%', weeklySummary.onTimeRate > 0.9 ? '✅' : '⚠️'],
    ['Schedule Efficiency', (weeklySummary.efficiency * 100).toFixed(2) + '%', weeklySummary.efficiency > 0.8 ? '✅' : '⚠️']
  ];
  
  sheet.getRange(row, 1, summaryData.length, summaryData[0].length).setValues(summaryData);
  
  return row + summaryData.length + 2;
}

function formatDashboard(sheet) {
  const range = sheet.getDataRange();
  sheet.autoResizeColumns(1, 8);
  range.setFontFamily('Arial').setFontSize(10);
  range.setBorder(true, true, true, true, true, true);
}

function parseDate(val) {
  if (val instanceof Date) return val;
  return new Date(val);
}

function calculateWeekSpan(data) {
  if (data.length === 0) return 0;
  const minStart = Math.min(...data.map(d => d.setupStart.getTime()));
  const maxEnd = Math.max(...data.map(d => d.runEnd.getTime()));
  const spanMs = maxEnd - minStart;
  return Math.ceil(spanMs / (86400000)); // days
}

function calculateTotalMachineHours(data) {
  return data.reduce((sum, d) => sum + (d.runEnd - d.runStart) / 3600000 + (d.setupEnd - d.setupStart) / 3600000, 0);
}

function calculateTotalSetupHours(data) {
  return data.reduce((sum, d) => sum + (d.setupEnd - d.setupStart) / 3600000, 0);
}

function calculateAvgSetupDuration(data) {
  const completed = data.length;
  const totalSetup = calculateTotalSetupHours(data);
  return completed > 0 ? totalSetup / completed : 0;
}

function parseDuration(str) {
  if (!str) return 0;
  let min = 0;
  str.split(' ').forEach(p => {
    if (p.endsWith('D')) min += parseInt(p.slice(0, -1)) * 1440;
    else if (p.endsWith('H')) min += parseInt(p.slice(0, -1)) * 60;
    else if (p.endsWith('M')) min += parseInt(p.slice(0, -1));
  });
  return min / 60; // to hours
}

function calculatePlannedHours(data) {
  let planned = 0;
  data.forEach(d => {
    const timing = d.timing;
    if (!timing) return;
    const totalStr = timing.split(' (')[0];
    let pausedStr = timing.substring(totalStr.length);
    let holidayStr = '';
    const holidayMatch = pausedStr.match(/paused ([\dDHM ]+) during holidays/);
    if (holidayMatch) holidayStr = holidayMatch[1];
    let gapStr = '';
    const gapMatch = pausedStr.match(/paused ([\dDHM ]+) due to shift gaps/);
    if (gapMatch) gapStr = gapMatch[1];
    const totalH = parseDuration(totalStr);
    const holidayH = parseDuration(holidayStr);
    const gapH = parseDuration(gapStr);
    planned += totalH - holidayH - gapH;
  });
  return planned;
}

function calculateDelayedBatches(data) {
  const batchMap = new Map();
  data.forEach(d => {
    if (!batchMap.has(d.batchId)) {
      batchMap.set(d.batchId, { maxEnd: d.runEnd, due: d.dueDate, partNumber: d.partNumber });
    } else {
      const info = batchMap.get(d.batchId);
      if (d.runEnd > info.maxEnd) info.maxEnd = d.runEnd;
    }
  });
  let delayedBatches = 0;
  const delayedList = [];
  batchMap.forEach((info, batchId) => {
    if (info.due && info.maxEnd > info.due) {
      delayedBatches++;
      delayedList.push({
        batchId,
        partNumber: info.partNumber,
        dueDate: info.due,
        runEnd: info.maxEnd,
        delayHours: (info.maxEnd - info.due) / 3600000
      });
    }
  });
  return { delayedBatches, delayedList };
}

// ==================== ANALYSIS FUNCTIONS ====================

function analyzePersonnelUtilization(data, weekSpan) {
  const personHours = {};
  const shiftHours = { 'Shift 1': {}, 'Shift 2': {} };
  
  data.forEach(item => {
    if (!item.person || item.person === '') return;
    
    const setupDuration = (item.setupEnd - item.setupStart) / 3600000;
    const shift = item.setupStart.getHours() < 14 ? 'Shift 1' : 'Shift 2';
    
    if (!personHours[item.person]) {
      personHours[item.person] = { active: 0 };
    }
    
    personHours[item.person].active += setupDuration;
    
    if (!shiftHours[shift][item.person]) {
      shiftHours[shift][item.person] = 0;
    }
    shiftHours[shift][item.person] += setupDuration;
  });
  
  const setupByShift = ['Shift 1', 'Shift 2'].map(shift => {
    const personsUsed = Object.keys(shiftHours[shift]).length;
    const totalAvailable = 2;
    return {
      name: shift,
      personsUsed,
      totalAvailable,
      utilization: personsUsed / totalAvailable
    };
  });
  
  const totalPossible = weekSpan > 0 ? weekSpan * 8 : 0;
  const personEfficiency = Object.keys(personHours).map(person => {
    const hours = personHours[person];
    const activeHours = hours.active;
    const utilization = totalPossible > 0 ? activeHours / totalPossible : 0;
    return {
      name: person,
      activeHours,
      totalHours: totalPossible,
      idleHours: Math.max(0, totalPossible - activeHours),
      utilization
    };
  });
  
  return { setupByShift, personEfficiency };
}

function analyzeMachineUtilization(data, weekSpan) {
  const machineStats = {};
  
  data.forEach(item => {
    if (!machineStats[item.machine]) {
      machineStats[item.machine] = {
        name: item.machine,
        runHours: 0,
        setupHours: 0,
        batches: 0
      };
    }
    
    const setupDuration = (item.setupEnd - item.setupStart) / 3600000;
    const runDuration = (item.runEnd - item.runStart) / 3600000;
    
    machineStats[item.machine].setupHours += setupDuration;
    machineStats[item.machine].runHours += runDuration;
    machineStats[item.machine].batches++;
  });
  
  const availableHours = weekSpan * 24;
  const machines = Object.values(machineStats).map(machine => {
    const totalHours = machine.runHours + machine.setupHours;
    const utilization = availableHours > 0 ? totalHours / availableHours : 0;
    return {
      ...machine,
      totalHours,
      utilization
    };
  });
  
  return { machines };
}

function analyzeShiftActivity(data) {
  const windows = [
    { start: 6, end: 9, name: '06:00-09:00' },
    { start: 9, end: 12, name: '09:00-12:00' },
    { start: 12, end: 15, name: '12:00-15:00' },
    { start: 15, end: 18, name: '15:00-18:00' },
    { start: 18, end: 21, name: '18:00-21:00' },
    { start: 21, end: 24, name: '21:00-24:00' }
  ];
  
  const resourceActivity = {};
  
  data.forEach(item => {
    const resources = [item.machine, item.person].filter(Boolean);
    
    resources.forEach(resource => {
      if (!resourceActivity[resource]) {
        resourceActivity[resource] = windows.map(w => ({ ...w, activeHours: 0 }));
      }
      
      windows.forEach((window, idx) => {
        const windowStart = new Date(item.setupStart);
        windowStart.setHours(window.start, 0, 0, 0);
        
        const windowEnd = new Date(item.setupStart);
        windowEnd.setHours(window.end, 0, 0, 0);
        
        const overlapStart = new Date(Math.max(item.setupStart.getTime(), windowStart.getTime()));
        const overlapEnd = new Date(Math.min(item.runEnd.getTime(), windowEnd.getTime()));
        
        if (overlapStart < overlapEnd) {
          const overlapHours = (overlapEnd - overlapStart) / 3600000;
          resourceActivity[resource][idx].activeHours += overlapHours;
        }
      });
    });
  });
  
  const heatmap = Object.keys(resourceActivity).map(resource => ({
    name: resource,
    windows: resourceActivity[resource],
    totalHours: resourceActivity[resource].reduce((sum, w) => sum + w.activeHours, 0)
  }));
  
  return { heatmap };
}

function findExceptions(data, weekSpan) {
  const { delayedList } = calculateDelayedBatches(data);
  const machineUtil = analyzeMachineUtilization(data, weekSpan);
  const personnelUtil = analyzePersonnelUtilization(data, weekSpan);
  
  const overutilized = [
    ...machineUtil.machines.filter(m => m.utilization > 0.9).map(m => ({
      name: m.name,
      type: 'Machine',
      utilization: m.utilization
    })),
    ...personnelUtil.personEfficiency.filter(p => p.utilization > 0.9).map(p => ({
      name: p.name,
      type: 'Person',
      utilization: p.utilization
    }))
  ];
  
  const underutilized = [
    ...machineUtil.machines.filter(m => m.utilization < 0.4 && m.utilization > 0).map(m => ({
      name: m.name,
      type: 'Machine',
      utilization: m.utilization
    })),
    ...personnelUtil.personEfficiency.filter(p => p.utilization < 0.4 && p.utilization > 0).map(p => ({
      name: p.name,
      type: 'Person',
      utilization: p.utilization
    }))
  ];
  
  return { delayed: delayedList, overutilized, underutilized };
}

function calculateWeeklySummary(data) {
  const totalOperations = data.length;
  const totalMachineHours = calculateTotalMachineHours(data);
  const totalSetupHours = calculateTotalSetupHours(data);
  const weekSpan = calculateWeekSpan(data);
  
  const machineUtil = analyzeMachineUtilization(data, weekSpan);
  const personnelUtil = analyzePersonnelUtilization(data, weekSpan);
  
  const avgMachineUtil = machineUtil.machines.length > 0 ? 
    machineUtil.machines.reduce((sum, m) => sum + m.utilization, 0) / machineUtil.machines.length : 0;
  
  const avgPersonUtil = personnelUtil.personEfficiency.length > 0 ?
    personnelUtil.personEfficiency.reduce((sum, p) => sum + p.utilization, 0) / personnelUtil.personEfficiency.length : 0;
  
  const { delayedBatches } = calculateDelayedBatches(data);
  const totalBatches = new Set(data.map(d => d.batchId)).size;
  const onTimeRate = totalBatches > 0 ? (totalBatches - delayedBatches) / totalBatches : 0;
  
  const efficiency = avgMachineUtil * 0.6 + avgPersonUtil * 0.4;
  
  return {
    totalOperations,
    totalMachineHours,
    totalSetupHours,
    avgMachineUtil,
    avgPersonUtil,
    onTimeRate,
    efficiency
  };
}

function formatDateTime(date) {
  if (!date) return '';
  return Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm');
}

/* === END OF FILE === */ 